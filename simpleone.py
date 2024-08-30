from collections import deque
import os
from functools import wraps
from Storages import SingletonKeyValueStorage
import math
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
import boto3

# S3 configuration
BUCKET_NAME = os.environ['AWS_S3_BUCKET_NAME']
UPLOAD_DIRECTORY = "./uploads"  # Local directory (optional)
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    region_name=os.environ['AWS_DEFAULT_REGION']
)
# storage
store = SingletonKeyValueStorage()
store.s3_backend(
            bucket_name = os.environ['AWS_S3_BUCKET_NAME'],
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
            region_name=os.environ['AWS_DEFAULT_REGION']
        )

class S3LargeUploadingModel:
    def __init__(self,file_name=None,file_size=0,file_hash=None,
                upload_id=None,chunk_size = 5 * 1024 * 1024,
                parts = [],) -> None:
        
        # Initialize the file name as None, to be set when the upload process starts
        self.file_name = file_name
        
        # Initialize the file size as None, to be set when the file information is provided
        self.file_size = file_size
        
        # Initialize the file hash as None, can be used for data integrity checks
        self.file_hash = file_hash
        
        # Initialize the upload ID as None, which will be assigned once the upload session is initiated
        self.upload_id = upload_id
        
        # Set the chunk size to 5 MB (5 * 1024 * 1024 bytes); this size dictates how large each part of the file upload will be
        self.chunk_size = chunk_size
        
        # Initialize the total number of chunks as None, which will be calculated based on the file size and chunk size
        self.total_chunks = math.ceil(file_size / chunk_size)
        
        # Initialize an empty list to hold parts metadata, containing dictionaries with PartNumber and ETag for each uploaded chunk
        self.parts:list[str,dict] = parts  # List of dictionaries: [{'PartNumber': xxx, 'ETag': xxx}]

        self.FSMs_state=S3LargeUploadingState.States.idle

    def is_recieved(self):
        return len(self.parts)==self.total_chunks

    # do not random gen id, for to identify file
    @staticmethod
    def gen_id(file_name,file_size,file_hash):
        return f'{file_name},{file_size},{file_hash}'
    
    def get_id(self):
        return S3LargeUploadingModel.gen_id(
                    self.file_name,self.file_size,self.file_hash)
    
    def to_dict(self):
        return self.__dict__
    
    def from_dict(self,data):
        for k in self.__dict__.keys():
            setattr(self,k,data[k])
        return self
    
class S3LargeUploadingController:
    def __init__(self,model:S3LargeUploadingModel) -> None:
        self.model = model
        self.fsm = S3LargeUploadingState(self)
    
    # public
    @staticmethod
    def new_or_find_file_uploading(file_name,file_size,file_hash):
        # find the file is recorded
        id = S3LargeUploadingModel.gen_id(file_name,file_size,file_hash)
        model_data = store.get(id)
        if model_data: return S3LargeUploadingController(
                                    S3LargeUploadingModel(
                                        ).from_dict(model_data))

        # new request for file uploading, hard operation
        response = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=file_name)
        upload_id = response['UploadId']    
        model = S3LargeUploadingModel(file_name,file_size,file_hash,upload_id)
        return S3LargeUploadingController(model).save_model()
    
    def save_model(self):
        store.set(self.model.get_id(),self.model.to_dict())
        return self
        
    def delete_model(self):
        return store.delete(self.model.get_id())
    
    def to_recieving(self,chunk_data):
        self.fsm.to_recieving(chunk_data)
    def to_recieved(self):
        self.fsm.to_recieved()
    def to_recieve_failure(self):
        self.fsm.to_recieve_failure()
    def to_merged(self):
        self.fsm.to_merged()
    def to_merge_failure(self):
        self.fsm.to_merge_failure()

    def current_state(self):
        return self.fsm._state
    
    def next_action(self,target_state):        
        return self.fsm.next_action(target_state)

    ################################# privates will use in it self or FSMs class
        
    def _merge_chunk(self):        
        # Complete the multipart upload
        response = s3_client.complete_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=self.model.file_name,
            UploadId=self.model.upload_id,
            MultipartUpload={'Parts': self.model.parts}
        )
        print(f"File {self.model.file_name} has been uploaded and merged on S3 successfully.")

    def _append_chunk(self,chunk_data):
        this_chunk_No = len(self.model.parts) + 1

        # Upload the chunk as a part of the multipart upload
        part_response = s3_client.upload_part(
            Bucket=BUCKET_NAME,
            Key=self.model.file_name,
            PartNumber=this_chunk_No,
            UploadId=self.model.upload_id,
            Body=chunk_data
        )

        # Store part information to complete the multipart upload later
        self.model.parts.append({
            'PartNumber': this_chunk_No,
            'ETag': part_response['ETag']
        })
        self.save_model()

        # Check if all chunks have been uploaded
        # if this_chunk_No == self.model.total_chunks:
        #     self._merge_chunk()

class S3LargeUploadingState:
    class States:
        idle = 'idle'
        recieving = 'recieving'
        recieved = 'recieved'
        recieve_failure = 'recieve_failure'
        merged = 'merged'
        merge_failure = 'merge_failure'

    _transitions = {
        States.idle:             [States.recieving],
        States.recieving:        [States.recieved, States.recieve_failure, States.idle],
        States.recieve_failure:  [States.recieving], # retry recieving
        States.recieved:         [States.merged, States.merge_failure],
        States.merge_failure:    [States.merged],
        States.merged:           [], # end of task life
        
    }
    _states = list(_transitions.keys())
    
    def __init__(self, controller:S3LargeUploadingController):
        self.controller = controller
        self.model = self.controller.model
        self._state = self.model.FSMs_state

    def set_state(self,state):
        self._state=state
        self.model.FSMs_state=state
        self.controller.save_model()
    
    def handle_errors(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self:S3LargeUploadingState=self
            valid_transitions = self._transitions[self._state]
            target_transition = func.__name__.replace('to_','')        
            if target_transition not in valid_transitions:            
                raise ValueError(f"Invalid transition from [{self._state}] -> [{target_transition}]")
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                print(f'[{self.__class__.__name__}]: {e}')
        return wrapper
    
    @handle_errors
    def to_recieving(self,chunk_data):
        # start recieving flows
        if chunk_data:
            self.set_state(S3LargeUploadingState.States.recieving)
            if not self.model.is_recieved():
                self.controller._append_chunk(chunk_data)

    @handle_errors
    def to_idle(self):
        self.set_state(S3LargeUploadingState.States.idle)

    @handle_errors
    def to_recieved(self):
        if self.model.is_recieved():
            self.set_state(S3LargeUploadingState.States.recieved)
        else:
            self.to_idle()
            
    @handle_errors
    def to_recieve_failure(self):
        self.set_state(S3LargeUploadingState.States.recieve_failure)

    @handle_errors
    def to_merged(self):
        try:
            self.controller._merge_chunk()
            self.set_state(S3LargeUploadingState.States.merged)
        except Exception as e:
            print(e)            
            self.to_merge_failure()

    @handle_errors
    def to_merge_failure(self):
        self.set_state(S3LargeUploadingState.States.merge_failure)
    
    def find_path(self, transitions:dict, start_state, end_state):
        queue = deque([[start_state]])    
        visited = set()    
        while queue:
            path = queue.popleft()
            state = path[-1]        
            if state == end_state:
                return path
            if state not in visited:
                visited.add(state)            
                next_states = transitions.get(state, [])
                for next_state in next_states:
                    new_path = list(path)
                    new_path.append(next_state)
                    queue.append(new_path)
        return []
    
    def next_action(self,target_state):
        path = self.find_path(self._transitions, self._state, target_state)
        if len(path)<=1: return None
        return path[1]

#####################################################################
# server by lib of fastapi
app = FastAPI()

@app.get("/")
def serve_html():
    return FileResponse("index.html")

def main_loop(file_name, file_size, file_hash, file=None):
    STATES = S3LargeUploadingState.States
    try:
        # Initialize the controller and determine the current state and next action
        controller = S3LargeUploadingController.new_or_find_file_uploading(file_name, file_size, file_hash)
        chunk_data = file.file.read() if file else None # Read the chunk data
        current = controller.current_state()
        action = controller.next_action(target_state=STATES.merged)

        if current == STATES.merged and action is None:
            # If already merged and no further action needed, finish
            print({'message': f'Current: {current}, finish!'})
            controller.delete_model()
            return JSONResponse(content={'message': f'Current: {current}, finish!'})

        print(f'Current: {current}, try to_{action}')
        
        # Perform the next action based on the current state        
        if action == STATES.recieving:
            if chunk_data is not None:
                getattr(controller, f'to_{action}')(chunk_data)
        else:
            getattr(controller, f'to_{action}')()

        return JSONResponse(content={
            'data': controller.model.to_dict(),
            'message': f'Current: {current}, try to_{action}'
        })
        
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/start_upload/")
def start_upload(
    file_name: str = Form(...),
    file_size: int = Form(...),
    file_hash: str = Form(...)):
    return main_loop(file_name, file_size, file_hash)


@app.post("/upload_chunk/")
def upload_chunk(
    file: UploadFile = File(...),
    file_name: str = Form(...),
    file_size: int = Form(...),
    file_hash: str = Form(...)):
    return main_loop(file_name, file_size, file_hash, file)

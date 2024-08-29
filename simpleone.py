from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
import boto3

from Storages import SingletonKeyValueStorage

app = FastAPI()

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

@app.get("/")
def serve_html():
    return FileResponse("index.html")  # Ensure the file is in the same directory as this script

@app.post("/start_upload/")
def start_upload(filename: str = Form(...),
                 total_chunks: int = Form(...)):
    file_meta = store.get(filename)
    if file_meta is None:
        # Start a new multipart upload
        response = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=filename)
        upload_id = response['UploadId']    
        file_meta = {'upload_id': upload_id,
                    'total_chunks':total_chunks,'parts': []}
        store.set(filename,file_meta)
    else:
        upload_id = file_meta['upload_id']

    return JSONResponse(content={"upload_id": upload_id, 'chunk_number':len(file_meta['parts']),
                                 "message": "Multipart upload initiated."})

@app.post("/upload_chunk/")
async def upload_chunk(
    file: UploadFile = File(...),
    filename: str = Form(...),
):
    file_meta = store.get(filename)
    if file_meta is None: raise ValueError(f'file of {filename} not start_upload yet!')
    upload_id = file_meta['upload_id']
    total_chunks = file_meta['total_chunks']
    try:
        # Read chunk data
        chunk_data = await file.read()

        # Upload the chunk as a part of the multipart upload
        part_response = s3_client.upload_part(
            Bucket=BUCKET_NAME,
            Key=filename,
            PartNumber=len(file_meta['parts']) + 1,
            UploadId=upload_id,
            Body=chunk_data
        )

        # Store part information to complete the multipart upload later
        file_meta['parts'].append({
            'PartNumber': len(file_meta['parts']) + 1,
            'ETag': part_response['ETag']
        })
        store.set(filename,file_meta)
        # Check if all chunks have been uploaded
        if len(file_meta['parts']) + 1 == total_chunks:
            # Complete the multipart upload
            complete_multipart_upload(filename, upload_id, file_meta['parts'])
            store.delete(filename)

            return JSONResponse(content={"filename": filename,
                                         "message": "All chunks uploaded and merged successfully on S3."})

        return JSONResponse(content={"filename": filename,
                                     "message": f"Chunk {len(file_meta['parts']) + 1} of {total_chunks} uploaded successfully."})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def complete_multipart_upload(filename: str, upload_id: str, parts=[]):
    """
    Complete the multipart upload by merging all uploaded parts.
    """
    parts = parts
    response = s3_client.complete_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=filename,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )

    print(f"File {filename} has been uploaded and merged on S3 successfully.")

# Run the application using: uvicorn filename:app --reload

from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
import boto3

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

# Store upload session information in memory or use persistent storage like Redis or DynamoDB for production
upload_sessions = {}

@app.get("/")
def serve_html():
    return FileResponse("index.html")  # Ensure the file is in the same directory as this script

@app.post("/start_upload/")
def start_upload(filename: str = Form(...)):
    # Start a new multipart upload
    response = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=filename)
    upload_id = response['UploadId']
    
    # Initialize upload session for tracking parts
    upload_sessions[filename] = {
        'upload_id': upload_id,
        'parts': []
    }
    
    return JSONResponse(content={"upload_id": upload_id, "message": "Multipart upload initiated."})

@app.post("/upload_chunk/")
async def upload_chunk(
    file: UploadFile = File(...),
    filename: str = Form(...),
    chunk_number: int = Form(...),
    total_chunks: int = Form(...),
    upload_id: str = Form(...)
):
    try:
        # Read chunk data
        chunk_data = await file.read()

        # Upload the chunk as a part of the multipart upload
        part_response = s3_client.upload_part(
            Bucket=BUCKET_NAME,
            Key=filename,
            PartNumber=chunk_number + 1,
            UploadId=upload_id,
            Body=chunk_data
        )

        # Store part information to complete the multipart upload later
        upload_sessions[filename]['parts'].append({
            'PartNumber': chunk_number + 1,
            'ETag': part_response['ETag']
        })

        # Check if all chunks have been uploaded
        if chunk_number + 1 == total_chunks:
            # Complete the multipart upload
            complete_multipart_upload(filename, upload_id)

            return JSONResponse(content={"filename": filename, "message": "All chunks uploaded and merged successfully on S3."})

        return JSONResponse(content={"filename": filename, "message": f"Chunk {chunk_number + 1} of {total_chunks} uploaded successfully."})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def complete_multipart_upload(filename: str, upload_id: str):
    """
    Complete the multipart upload by merging all uploaded parts.
    """
    parts = upload_sessions[filename]['parts']
    response = s3_client.complete_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=filename,
        UploadId=upload_id,
        MultipartUpload={'Parts': parts}
    )

    # Optionally, cleanup session data
    del upload_sessions[filename]

    print(f"File {filename} has been uploaded and merged on S3 successfully.")

# Run the application using: uvicorn filename:app --reload

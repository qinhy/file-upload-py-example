from fastapi.responses import FileResponse, JSONResponse
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
import os

app = FastAPI()

# Directory where uploaded files will be saved
UPLOAD_DIRECTORY = "./uploads"

# Ensure the upload directory exists
os.makedirs(UPLOAD_DIRECTORY, exist_ok=True)

# Serve the HTML page
@app.get("/")
def serve_html():
    return FileResponse("index.html")  # Ensure the file is in the same directory as this script

@app.post("/upload/")
async def upload_large_file(file: UploadFile = File(...)):
    try:
        # Set the file location to save the uploaded file
        file_location = os.path.join(UPLOAD_DIRECTORY, file.filename)

        # Open the file in write-binary mode and write in chunks
        with open(file_location, "wb") as buffer:
            # Use a small buffer size to read the file in chunks (e.g., 1MB)
            chunk_size = 5 * 1024 * 1024
            while content := await file.read(chunk_size):
                buffer.write(content)
        return JSONResponse(content={"filename": file.filename, "message": "File uploaded successfully."})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload_chunk/")
async def upload_chunk( file: UploadFile = File(...),
                        filename: str = Form(...),
                        chunk_number: int = Form(...),
                        total_chunks: int = Form(...)
                    ):
    try:
        # Save each chunk independently with a unique name based on chunk number
        chunk_filename = os.path.join(UPLOAD_DIRECTORY, f"{filename}.chunk_{chunk_number}")

        # Write the chunk to a file
        with open(chunk_filename, "wb") as buffer:
            buffer.write(await file.read())

        # Check if all chunks have been uploaded
        if chunk_number + 1 == total_chunks:
            # Merge chunks into a single file
            merge_chunks(filename, total_chunks)
            return JSONResponse(content={"filename": filename, "message": "All chunks uploaded successfully."})
        return JSONResponse(content={"filename": filename, "message": f"Chunk {chunk_number + 1} of {total_chunks} uploaded successfully."})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def merge_chunks(filename: str, total_chunks: int):
    """
    Merge the uploaded chunk files into a single file.
    """
    final_file_path = os.path.join(UPLOAD_DIRECTORY, filename)
    
    # Open the final file in write-binary mode
    with open(final_file_path, "wb") as final_file:
        # Read each chunk and append it to the final file
        for chunk_number in range(total_chunks):
            chunk_path = os.path.join(UPLOAD_DIRECTORY, f"{filename}.chunk_{chunk_number}")
            with open(chunk_path, "rb") as chunk_file:
                final_file.write(chunk_file.read())
            # Optionally, remove the chunk after merging
            os.remove(chunk_path)
    
    print(f"File {filename} has been merged successfully.")
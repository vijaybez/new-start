This is a script that interacts with AWS S3 using boto3 package. The code is designed to perform basic S3 functions such are creating and deleting buckets and files in S3.
We can also set certain security options such as blocking public access on buckets.
it also uses the try except defination for every method to capture any exceptions and print them on teh screen. The actual production version of this code would log the error to a log file

This was run in a virtual environment using PycHarm and setting up environmental variables for ACcess and Secret Key.
Here are the list of methods:

create_bucket
block_public_access
list_objects
copy_file
generate_presigned_url
upload_files
download_files
delete_files
delete_bucket
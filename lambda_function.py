import json
import boto3
import urllib.parse
import time

# Initialize AWS clients
sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
transcribe_client = boto3.client('transcribe')

# Lambda handler function
def lambda_handler(event, context):
    # Extract the bucket and file details from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    #  file upload sns
    sns_client.publish(
            TopicArn='your-topc-arn', // for sns notificcation
            Message=json.dumps({
            'bucket':source_bucket,
            'job_id': file_key,
            'status': 'Upload Successful',
               
         }),
            Subject='Audio/Video File Upload Successful'
        )
    
    # Define the S3 URI for the video file
    file_uri = f's3://{source_bucket}/{file_key}'
    
    # Generate a unique name for the transcription job
    transcription_job_name = f"transcription-{int(time.time())}"
    
    # Start transcription job with Amazon Transcribe (including speaker diarization)
    try:
        response = transcribe_client.start_transcription_job(
            TranscriptionJobName=transcription_job_name,
            Media={'MediaFileUri': file_uri},
            MediaFormat=file_key.split('.')[-1],  # Infer file format from the key
            LanguageCode='en-US',  # Set the appropriate language code
            OutputBucketName='extractedtextimage',  # The S3 bucket to save the transcription
            Settings={
                'ShowSpeakerLabels': True,  # Enable speaker diarization
                'MaxSpeakerLabels': 5  # Specify the maximum number of speakers (adjust as needed)
            }
        )
        print(f"Transcription job {transcription_job_name} with speaker diarization started successfully.")
    
    except Exception as e:
        print(f"Error starting transcription job: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error starting transcription job: {e}")
        }
    
    # Wait for the transcription job to complete
    while True:
        status = transcribe_client.get_transcription_job(TranscriptionJobName=transcription_job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Transcription in progress...")
        time.sleep(5)  # Sleep for 5 seconds before checking again
    
    # Check if the transcription job succeeded
    if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
        # Get the transcription result URL
        transcript_url = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
        print(f"Transcription completed: {transcript_url}")
        
        # Fetch the transcribed text from S3
        transcript_response = s3_client.get_object(
            Bucket='extractedtextimage',
            Key=f"{transcription_job_name}.json"
        )
        transcript_content = json.loads(transcript_response['Body'].read().decode('utf-8'))
        
        # Initialize an empty list to store the ordered conversation
        conversation = []

        # Map each segment of text with its corresponding speaker
        speaker_label_map = {}
        for segment in transcript_content['results']['speaker_labels']['segments']:
            for item in segment['items']:
                speaker_label_map[item['start_time']] = segment['speaker_label']

        # Combine the speaker labels with their corresponding text in the order they occur
        for item in transcript_content['results']['items']:
            if 'start_time' in item:
                speaker_label = speaker_label_map.get(item['start_time'], "unknown_speaker")
                text = item['alternatives'][0]['content']
                if len(conversation) > 0 and conversation[-1]['speaker'] == speaker_label:
                    # Append to the last speaker's sentence
                    conversation[-1]['text'] += f" {text}"
                else:
                    # Add new speaker turn
                    conversation.append({'speaker': speaker_label, 'text': text})

        # Format the transcript for readable output
        formatted_transcript = ""
        for turn in conversation:
            formatted_transcript += f"**{turn['speaker']}**:{turn['text']}\n\n"

        # Save the formatted transcript as a .txt file in the same bucket
        txt_file_key = f"{file_key.rsplit('.', 1)[0]}.txt"
        s3_client.put_object(
            Bucket='extractedtextimage',
            Key=txt_file_key,
            Body=formatted_transcript
        )
        
        # Delete the specific transcription JSON file after saving the .txt file
        try:
            json_file_key = f"{transcription_job_name}.json"
            s3_client.delete_object(Bucket='extractedtextimage', Key=json_file_key)
            print(f"Deleted temporary file: {json_file_key}")
        except Exception as e:
            print(f"Error deleting JSON file: {e}")
        
        # Return success
        
        # my sns notification
        sns_client.publish(
            TopicArn='your-topic-arn',
            Message=json.dumps({
            'bucket':'extractedtextimage',
            'job_id': file_key,
            'status': 'Extraction Completed',
               
         }),
            Subject='Audio/Video Extraction Completed'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Transcription with speaker diarization successful! Transcription saved to {txt_file_key}")
        }
    else:
        sns_client.publish(
            TopicArn='your-topic-arn',
            Message=json.dumps({
            'bucket':source_bucket,
            'job_id': file_key,
            'status': 'Extraction Failed',
               
         }),
            Subject='Audio/Video Extraction Failed'
        )
        # Return failure
        print(f"Transcription job {transcription_job_name} failed.")
        return {
            'statusCode': 500,
            'body': json.dumps("Transcription job failed.")
        }

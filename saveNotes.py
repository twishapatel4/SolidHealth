import json
import base64

file = 'new_data.json'
# file = 'getNotes.json'

def get_notes(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return

    found_doc = False
    resources = data if isinstance(data, list) else [data]
    for item in resources:
       # Check if the resource is a DocumentReference
        if item.get('resourceType') == 'DocumentReference':
            found_doc = True
            doc_id = item.get('id', 'Unknown ID')
            print(f"\n[Document ID: {doc_id}]")
            print(f"Resource Type: {item.get('resourceType')}")
            
            # Navigate to content -> attachment -> data
            contents = item.get('content', [])
            for i, content_item in enumerate(contents):
                attachment = content_item.get('attachment', {})
                base64_data = attachment.get('data')
                attachment_url = attachment.get('url')

                if base64_data:
                    try:
                        # Decode Base64 to Bytes, then Bytes to String
                        decoded_bytes = base64.b64decode(base64_data)
                        decoded_text = decoded_bytes.decode('utf-8')
                        
                        print(f"--- Decoded Note ---")
                        print(decoded_text)
                    except Exception as e:
                        print(f"Error decoding Base64 for Doc {doc_id}: {e}")

                elif attachment_url:
                    get_data_from_url(attachment_url)

                else:
                    print(f"No data or URL found in attachment {i+1} for Document {doc_id}")
    
    if not found_doc:
        print("No DocumentReference resources found.")

def get_data_from_url(url):
    # Write code to fetch data from the URL if needed
    url=f"https://fhir.careevolution.com/Master.Adapter1.WebClient/api/fhir-r4/{url}"
    print(url)
    

if __name__ == "__main__":
    get_notes(file)
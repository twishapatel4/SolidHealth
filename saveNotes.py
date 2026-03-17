import json
import base64

def get_notes(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return

    found_doc = False
    # Ensure resources is a list
    resources = data if isinstance(data, list) else [data]
    
    for item in resources:
        # 1. Look for DocumentReference
        if item.get('resourceType') == 'DocumentReference':
            found_doc = True
            doc_id = item.get('id', 'Unknown ID')
            print(f"\n[Document ID: {doc_id}]")
            
            contents = item.get('content', [])
            for i, content_item in enumerate(contents):
                attachment = content_item.get('attachment', {})
                base64_data = attachment.get('data')
                attachment_url = attachment.get('url')

                # CASE A: Data is directly inside the DocumentReference
                if base64_data:
                    print(f"--- Decoded Note (Inline) ---")
                    process_and_decode(base64_data, attachment.get('contentType', 'utf-8'))

                # CASE B: Only a URL is present, look for the matching Binary resource in the same file
                elif attachment_url:
                    print(f"Searching for Binary resource matching URL: {attachment_url}")
                    url=f"https://fhir.careevolution.com/Master.Adapter1.WebClient/api/fhir-r4/{attachment_url}"  # Ensure URL is in the correct format for searching
                    binary_resource = find_binary_resource(resources, url)
                    
                    if binary_resource:
                        # print(f"--- Decoded Note (From Binary Resource: {binary_resource.get('id')}) ---")
                        process_and_decode(binary_resource.get('data'), binary_resource.get('contentType'))
                    else:
                        print(f"Could not find a matching Binary resource in the file for URL: {attachment_url}")

                else:
                    print(f"No data or URL found in attachment {i+1} for Document {doc_id}")
    
    if not found_doc:
        print("No DocumentReference resources found.")

def find_binary_resource(resources, url):
    """
    Searches the list of resources for a Binary resource that matches the provided URL.
    Checks both the 'id' (extracted from URL) and 'meta.source'.
    """
    # Standard FHIR URLs often look like "Binary/ID_VALUE"
    # We extract the ID part just in case
    target_id = url.split('/')[-1] 

    for item in resources:
        if item.get('resourceType') == 'Binary':
            # Check if ID matches or if the meta source URL matches exactly
            item_id = item.get('id')
            item_source = item.get('meta', {}).get('source', '')
            
            if item_id == target_id or item_source == url:
                # print(f"")
                return item
    return None

def process_and_decode(base64_data, content_type):
    """
    Decodes base64 data using the charset specified in the contentType.
    """
    if not base64_data:
        print("No data to decode.")
        return

    # Default to utf-8, but check if charset is specified (e.g., iso8859-1)
    encoding = 'utf-8'
    if content_type and 'charset=' in content_type:
        encoding = content_type.split('charset=')[-1].strip()

    try:
        decoded_bytes = base64.b64decode(base64_data)
        decoded_text = decoded_bytes.decode(encoding)
        print(decoded_text)
    except Exception as e:
        print(f"Error decoding data with encoding {encoding}: {e}")

if __name__ == "__main__":
    file = 'new_data.json' 
    get_notes(file)
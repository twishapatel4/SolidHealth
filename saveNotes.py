import json
import base64
from bs4 import BeautifulSoup 

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
            
            type=item.get('type', {})
            coding=type.get('coding', [])
            if coding:
                code=coding[0].get('code')

            contents = item.get('content', [])
            for i, content_item in enumerate(contents):
                attachment = content_item.get('attachment', {})
                base64_data = attachment.get('data')
                attachment_url = attachment.get('url')

                # CASE A: Data is directly inside the DocumentReference
                if base64_data:
                    print(f"--- Decoded Note (Inline) ---")
                    data=process_and_decode(base64_data, attachment.get('contentType', 'utf-8'))

                # CASE B: Only a URL is present, look for the matching Binary resource in the same file
                elif attachment_url:
                    url=f"https://fhir.careevolution.com/Master.Adapter1.WebClient/api/fhir-r4/{attachment_url}"  
                    # Ensure URL is in the correct format for searching
                    binary_resource = find_binary_resource(resources, url)
                    
                    if binary_resource:
                        data=process_and_decode(binary_resource.get('data'), binary_resource.get('contentType'))
                    else:
                        print(f"Could not find a matching Binary resource in the file for URL: {attachment_url}")

                else:
                    print(f"No data or URL found in attachment {i+1} for Document {doc_id}")

            save_decoded_note_to_file(data, doc_id, attachment.get('contentType', 'unknown'), code) 
    if not found_doc:
        print("No DocumentReference resources found.")

def find_binary_resource(resources, url):
    """
    Searches the list of resources for a Binary resource that matches the provided URL.
    Checks both the 'id' (extracted from URL) and 'meta.source'.
    """
    target_id = url.split('/')[-1] 

    for item in resources:
        if item.get('resourceType') == 'Binary':
            # Check if ID matches or if the meta source URL matches exactly
            item_id = item.get('id')
            item_source = item.get('meta', {}).get('source', '')
            
            if item_id == target_id or item_source == url:
                return item
    return None

def process_and_decode(base64_data, content_type):
    """
    Decodes Base64 and handles the content based on the contentType.
    """
    if not base64_data:
        print("Result: No data found to decode.")
        return

    # 1. Always decode the Base64 layer first to get raw bytes
    try:
        decoded_bytes = base64.b64decode(base64_data)
    except Exception as e:
        print(f"Error decoding Base64 string: {e}")
        return

    # 2. Logic based on Content-Type
    try:
        # Handle Plain Text
        if 'text/plain' in content_type:
            # Extract charset if present (e.g., iso8859-1), otherwise default to utf-8
            encoding = 'utf-8'
            if 'charset=' in content_type:
                encoding = content_type.split('charset=')[-1].strip()
            
            decoded_text = decoded_bytes.decode(encoding)
            return decoded_text

        # Handle XML
        elif 'application/xml' in content_type or 'text/xml' in content_type:
            # XML is usually UTF-8, but let's try to decode it safely
            # decoded_xml = decoded_bytes.decode('utf-8')
            pure_text = extract_text_from_xml(decoded_bytes)
            return pure_text

        # Handle other types (PDF, Images, etc.)
        else:
            print(f"--- Raw Data (Type: {content_type}) ---")
            # For non-text types, we usually don't print the whole thing to console
            return decoded_bytes.decode('latin-1')  # Return raw bytes as text for reference

    except UnicodeDecodeError:
        print("Error: Could not decode bytes into text. The data might be a non-text format (like PDF or Image).")
    except Exception as e:
        print(f"Error during processing: {e}")

    print("-" * 40)

def save_decoded_note_to_file(decoded_text, doc_id, content_type, code):
    """
    Saves the decoded text to a file named after the document ID and content type.
    """
    # Sanitize content type for filename (e.g., text/plain -> text_plain)
    safe_content_type = content_type.replace('/', '_').replace(' ', '_')
    filename = f"{doc_id}_{safe_content_type}.txt"
    
    #choose folder based on code if cda it should be inside cda_data else in the data folder at root
    # the cda_data and data folders are already created in the root of the project
    if code and 'cda' in code.lower():
        filename = f"cda_data/{filename}"
    else:
        filename = f"data/{filename}"

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(decoded_text)
        print(f"Decoded note saved to {filename}")
    except Exception as e:
        print(f"Error saving decoded note to file: {e}")

def extract_text_from_xml(xml_content):
    """
    Parses XML and returns human-readable text.
    Targeting titles, tables, and paragraphs.
    """
    try:
        # Use lxml-xml to handle the medical namespaces properly
        soup = BeautifulSoup(xml_content, 'xml')

        # 1. Clean up: Remove metadata tags that aren't useful in a text note
        for tag in soup(['templateId', 'id', 'code', 'effectiveTime', 'realmCode', 'typeId', 'confidentialityCode']):
            tag.decompose()

        # 2. Extract Text with formatting logic
        # We use a separator to ensure words from different tags don't mash together
        # (e.g., <td>Date</td><td>Problem</td> -> "Date Problem" instead of "DateProblem")
        text = soup.get_text(separator=' ', strip=True)

        # 3. Optional: More refined extraction (focus on Sections)
        # If the output is too noisy, you can target specific content:
        sections = []
        for section in soup.find_all('section'):
            title = section.find('title')
            content = section.find('text')
            if title:
                sections.append(f"\n--- {title.get_text().upper()} ---")
            if content:
                # Replace table tags with newlines to keep rows separate
                for tr in content.find_all('tr'):
                    tr.insert_after(soup.new_string('\n'))
                for td in content.find_all('td'):
                    td.insert_after(soup.new_string('  |  '))
                
                sections.append(content.get_text(strip=False))
        
        if sections:
            return "\n".join(sections)
        
        return text # Fallback to general text extraction

    except Exception as e:
        return f"Error parsing XML text: {e}"

if __name__ == "__main__":
    file = 'new_data.json' 
    get_notes(file)
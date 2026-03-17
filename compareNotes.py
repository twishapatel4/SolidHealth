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

    resources = data if isinstance(data, list) else [data]
    
    for item in resources:
        res_type = item.get('resourceType')

        # --- Handle DocumentReference ---
        # if res_type == 'DocumentReference':
        #     doc_id = item.get('id', 'Unknown ID')
        #     print(f"\n[DocumentReference ID: {doc_id}]")
            
        #     # 1. Extract Type Code
        #     doc_type_obj = item.get('type', {})
        #     codings = doc_type_obj.get('coding', [])
        #     if codings:
        #         print(f"Type Code: {codings[0].get('code')}")

        #     # 2. Path: content -> attachment -> (data/url)
        #     contents = item.get('content', [])
        #     for content_item in contents:
        #         attachment = content_item.get('attachment', {})
        #         # Pass the attachment object to the processor
        #         data=process_attachment_logic(attachment, resources)

        # --- Handle DiagnosticReport ---
        if res_type == 'DiagnosticReport':
            report_id = item.get('id', 'Unknown ID')
            # print(f"\n[DiagnosticReport ID: {report_id}]")s
            
            # Path: presentedForm -> (data/url)  <-- No "attachment" wrapper here!
            forms = item.get('presentedForm', [])
            for form_item in forms:
                # In DiagnosticReport, form_item IS the attachment-like object
                data=process_attachment_logic(form_item, resources)
                save_decoded_note_to_file(data, report_id, form_item.get('contentType', 'unknown'), None)

# This helper replaces the redundant if/else logic for both resource types
def process_attachment_logic(attachment_obj, resources):
    base64_data = attachment_obj.get('data')
    attachment_url = attachment_obj.get('url')
    content_type = attachment_obj.get('contentType', '')

    if base64_data:
        data=process_and_decode(base64_data, content_type)
    elif attachment_url:
        # print(f"Fetching from Binary: {attachment_url}")
        url=f"https://fhir.careevolution.com/Master.Adapter1.WebClient/api/fhir-r4/{attachment_url}"
        binary_res = find_binary_resource(resources, url)
        if binary_res:
            data=process_and_decode(binary_res.get('data'), binary_res.get('contentType'))
        else:
            print(f"Binary resource {attachment_url} not found in file.")
    return data
    
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
    filename = f"compare_{doc_id}_{safe_content_type}.txt"
    
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
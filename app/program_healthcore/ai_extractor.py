import os
import time
import json
import google.generativeai as genai
from flask import current_app

def extract_health_document(filepath):
    """
    Extracts structured data from a health document using Gemini.
    Returns a dict containing 'document_type' and the specific structured data.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"error": "GEMINI_API_KEY not configured."}
        
    try:
        genai.configure(api_key=api_key)
        
        # Upload the file to Gemini
        uploaded_file = genai.upload_file(path=filepath)
        
        # Wait for processing (important for PDFs)
        while uploaded_file.state.name == "PROCESSING":
            time.sleep(2)
            uploaded_file = genai.get_file(uploaded_file.name)
            
        if uploaded_file.state.name == "FAILED":
            return {"error": "Document processing failed in Gemini API."}

        # Initialize the model
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        prompt = """
        You are an expert AI Health Document Extractor. Your job is to extract structured medical information from the provided health document.
        Do NOT diagnose, interpret, or hallucinate. Return ONLY raw structured JSON data based on what is exactly written in the document.

        First, identify the document type. The allowed document types are:
        - "laboratory" (for blood tests, urine tests, pathology, etc.)
        - "medication" (for prescriptions, medication lists, discharge pharmacy)
        - "imaging" (for MRI, CT Scan, X-Ray, Ultrasound reports)
        - "timeline" (for surgery reports, hospital discharge summaries, diagnosis letters)
        - "other" (if it doesn't fit the above)

        Then, extract the relevant fields for that document type.

        If it's "laboratory", output exactly in this JSON format:
        {
          "document_type": "laboratory",
          "laboratory": "Name of Lab (if present)",
          "report_date": "YYYY-MM-DD",
          "tests": [
            {
              "name": "Test Name",
              "value": "Numeric value or result",
              "unit": "Unit (e.g. mg/dL)",
              "reference_range": "Range (if present)",
              "status": "High/Low/Normal (if indicated)"
            }
          ]
        }

        If it's "medication", output exactly in this JSON format:
        {
          "document_type": "medication",
          "report_date": "YYYY-MM-DD",
          "medications": [
            {
              "name": "Medication Name",
              "dose": "Dosage (e.g. 500 mg)",
              "frequency": "Frequency (e.g. Twice daily)",
              "status": "Active or Discontinued (if known)"
            }
          ]
        }
        
        If it's "imaging", output exactly in this JSON format:
        {
          "document_type": "imaging",
          "report_date": "YYYY-MM-DD",
          "modality": "e.g. MRI, CT Scan",
          "body_part": "e.g. Brain, Chest",
          "findings": "Summary of findings",
          "impression": "Radiologist impression"
        }

        Return ONLY the JSON string. Do not include markdown code blocks like ```json ... ```. 
        """

        response = model.generate_content([uploaded_file, prompt])
        
        # Clean up the file from Gemini
        try:
            genai.delete_file(uploaded_file.name)
        except:
            pass

        # Parse JSON
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        elif response_text.startswith("```"):
            response_text = response_text.replace("```", "").strip()
            
        try:
            structured_data = json.loads(response_text)
            return structured_data
        except json.JSONDecodeError:
            return {"error": "Failed to parse JSON from AI response.", "raw_response": response_text}

    except Exception as e:
        print(f"Gemini Extraction Error: {str(e)}")
        return {"error": str(e)}

import os
import base64
import google.generativeai as genai

def test():
    import dotenv
    dotenv.load_dotenv(r"d:\Users\yeshk\Documents\ait_platform\.env")
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"), transport="rest")
    
    img_path = r"d:\Users\yeshk\Documents\ait_platform\app\data\dbe_papers\images\cached_page_0.jpg"
    with open(img_path, "rb") as f:
        img_data = base64.b64encode(f.read()).decode("utf-8")
        
    prompt = "Describe this image."
    contents = [
        {
            "parts": [
                {"inline_data": {"mime_type": "image/jpeg", "data": img_data}},
                {"text": prompt}
            ]
        }
    ]
    
    try:
        model = genai.GenerativeModel("gemini-flash-latest")
        response = model.generate_content(contents)
        print("SUCCESS!")
        print(response.text)
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    test()

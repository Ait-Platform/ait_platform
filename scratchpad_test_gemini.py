import os
import google.generativeai as genai
from PIL import Image

def test():
    import dotenv
    dotenv.load_dotenv(r"d:\Users\yeshk\Documents\ait_platform\.env")
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    
    img_path = r"d:\Users\yeshk\Documents\ait_platform\app\data\dbe_papers\images\cached_page_0.jpg"
    pil_img = Image.open(img_path)
    
    prompt = "Describe this image."
    
    try:
        model = genai.GenerativeModel("gemini-flash-latest")
        response = model.generate_content([pil_img, prompt])
        print("SUCCESS!")
        print(response.text)
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    test()

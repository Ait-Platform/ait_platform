import os
import google.generativeai as genai

def test():
    import dotenv
    dotenv.load_dotenv(r"d:\Users\yeshk\Documents\ait_platform\.env")
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    
    try:
        models = genai.list_models()
        for m in models:
            print(m.name, m.supported_generation_methods)
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    test()

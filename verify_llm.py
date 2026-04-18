import os
from dotenv import load_dotenv
import google.generativeai as genai

# Load .env
load_dotenv()

def test_gemini():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("❌ GEMINI_API_KEY not found in .env")
        return

    print(f"Using API Key: {api_key[:5]}...{api_key[-5:]}")
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content("Say 'Gemini is connected' if you can hear me.")
        print(f"✅ Success! Gemini says: {response.text.strip()}")
    except Exception as e:
        print(f"❌ Gemini connection failed: {e}")

if __name__ == "__main__":
    test_gemini()

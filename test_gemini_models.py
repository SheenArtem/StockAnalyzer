
import google.generativeai as genai
import os

# User's key
api_key = "AIzaSyDY6PzZhghEKoKcsOULLPXPCCKpKWd8xwI"
genai.configure(api_key=api_key)

print("Listing available models...")
try:
    for m in genai.list_models():
        if 'generateContent' in m.supported_generation_methods:
            print(f"- {m.name}")
except Exception as e:
    print(f"Error: {e}")

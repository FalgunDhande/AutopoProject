import requests

def llama(prompt):
    try:
        res = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "qwen2.5:7b",
                "prompt": prompt,
                "stream": False
            },
            timeout=60
        )
        return res.json().get("response", "").strip()
    except Exception as e:
        print("🔥 LLM Error:", e)
        return "Error"

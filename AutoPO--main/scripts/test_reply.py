
from reply_listener import classify_intent

test_cases = [
    "Yes, please ship whatever you have.",
    "Go ahead with the partial order.",
    "No, I need the full shipment only. Cancel it.",
    "Ship the available items.",
    "Please wait until everything is in stock.",
    "Whatever is available is fine.",
    "Do not ship unless complete.",
    "Proceed."
]

print("--- Testing LLM Intent Classification ---")
for text in test_cases:
    intent = classify_intent(text)
    print(f"'{text}' \n   -> {intent}\n")

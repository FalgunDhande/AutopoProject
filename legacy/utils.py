def check_inventory(requested, available):
    if available >= requested:
        return "full"
    if available == 0:
        return "none"
    return "partial"

def send_mock_email(subject, body):
    print("\n==============================")
    print("📧 SUBJECT:", subject)
    print(body)
    print("==============================\n")

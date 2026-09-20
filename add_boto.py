with open("requirements.txt", "r", encoding="utf-8") as f:
    text = f.read()

if "boto3" not in text:
    with open("requirements.txt", "a", encoding="utf-8") as f:
        f.write("\nboto3==1.34.0\n")
    print("Added boto3 to requirements.txt")

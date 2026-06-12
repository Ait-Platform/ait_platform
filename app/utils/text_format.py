# app/utils/text_format.py
def _chunk_to_words(n: int) -> str:
    ones = ["zero","one","two","three","four","five","six","seven","eight","nine",
            "ten","eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen"]
    tens = ["","", "twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"]

    if n < 20:
        return ones[n]
    if n < 100:
        t, r = divmod(n, 10)
        return tens[t] + (f"-{ones[r]}" if r else "")
    h, r = divmod(n, 100)
    return ones[h] + " hundred" + (f" and {_chunk_to_words(r)}" if r else "")

def number_to_words(n: int) -> str:
    if n == 0:
        return "Zero"
    if n < 0:
        return "Minus " + number_to_words(-n)

    words = []
    thousands, rem = divmod(n, 1000)
    if thousands:
        words.append(_chunk_to_words(thousands) + " thousand")
    if rem:
        words.append(_chunk_to_words(rem))
    s = " ".join(words)
    return s[:1].upper() + s[1:]

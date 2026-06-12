import math

def number_to_words(n: int) -> str:
    """
    Convert an integer into English words.
    Example: 123 -> 'one hundred twenty-three'
    """
    # Simple implementation; for production you might use `num2words` library
    units = ["zero","one","two","three","four","five","six","seven","eight","nine"]
    teens = ["ten","eleven","twelve","thirteen","fourteen","fifteen",
             "sixteen","seventeen","eighteen","nineteen"]
    tens = ["","","twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"]

    if n < 10:
        return units[n]
    elif n < 20:
        return teens[n-10]
    elif n < 100:
        return tens[n//10] + ("" if n % 10 == 0 else "-" + units[n % 10])
    elif n < 1000:
        return units[n//100] + " hundred" + ("" if n % 100 == 0 else " " + number_to_words(n % 100))
    else:
        return str(n)  # fallback for larger numbers

def price_cents_for(amount: float) -> int:
    """
    Convert a float amount into integer cents.
    Example: 12.34 -> 1234
    """
    return int(round(amount * 100))
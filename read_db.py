import sqlite3

def check_questions():
    try:
        conn = sqlite3.connect('db.sqlite3')
        cursor = conn.cursor()
        
        print("--- CHAPTER 18 ---")
        cursor.execute("SELECT id, question FROM home_questions WHERE chapter_id = 18")
        for q_id, q_text in cursor.fetchall():
            print(f"Q: {q_text}")
            cursor.execute("SELECT option_text, sort_order FROM home_question_options WHERE question_id = ? ORDER BY sort_order", (q_id,))
            for opt, order in cursor.fetchall():
                print(f"  - {opt}")
                
        print("\n--- CHAPTER 20 ---")
        cursor.execute("SELECT id, question FROM home_questions WHERE chapter_id = 20")
        for q_id, q_text in cursor.fetchall():
            print(f"Q: {q_text}")
            cursor.execute("SELECT option_text, sort_order FROM home_question_options WHERE question_id = ? ORDER BY sort_order", (q_id,))
            for opt, order in cursor.fetchall():
                print(f"  - {opt}")
                
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_questions()

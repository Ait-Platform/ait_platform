import psycopg2

DATABASE_URL = "postgresql://ait_platform_db_user:b5LcEVWQeG0JyI6Vklo7zaQBZ1zsAfqj@dpg-d4bkqsf5r7bs73989ia0-a.oregon-postgres.render.com:5432/ait_platform_db"

def update_render_db():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Update mechanic
        cursor.execute("""
            UPDATE auth_subject 
            SET show_on_welcome = true,
                about_endpoint = 'mechanic_bp.about',
                name = 'Home Mechanic CRM',
                pay_endpoint = 'paddle_bp.paddle_start'
            WHERE slug = 'mechanic';
        """)

        # Update practice_crm
        cursor.execute("""
            UPDATE auth_subject 
            SET show_on_welcome = true,
                about_endpoint = 'practice_crm_bp.about',
                name = 'Health Practice CRM',
                pay_endpoint = 'paddle_bp.paddle_start'
            WHERE slug = 'practice_crm';
        """)
        
        # Update home
        cursor.execute("""
            UPDATE auth_subject 
            SET show_on_welcome = true,
                about_endpoint = 'home_bp.price_page'
            WHERE slug = 'home';
        """)

        conn.commit()
        print("Render DB updated successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    update_render_db()

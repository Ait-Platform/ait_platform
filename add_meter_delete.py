import re

# 1. Update routes.py
with open(r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py", "r", encoding="utf-8") as f:
    routes = f.read()

delete_logic = """
        # Handle Deleted Meters
        deleted_meters_json = request.form.get("deleted_meters_json")
        if deleted_meters_json:
            try:
                deleted_ids = json.loads(deleted_meters_json)
                for m_id in deleted_ids:
                    meter = BilMeter.query.get(m_id)
                    if meter and meter.sectional_unit_id == unit.id:
                        # Optional: also delete linked consumptions if cascade is not set
                        from app.models.billing import BilConsumption
                        BilConsumption.query.filter_by(meter_id=meter.id).delete()
                        db.session.delete(meter)
            except Exception as e:
                print(f"Error deleting meters: {e}")
                
        # 2. Handle New Meters
"""
routes = routes.replace("# 2. Handle New Meters", delete_logic)

with open(r"D:\Users\yeshk\Documents\ait_platform\app\school_billing\routes.py", "w", encoding="utf-8") as f:
    f.write(routes)

# 2. Update edit_property.html
with open(r"D:\Users\yeshk\Documents\ait_platform\templates\school_billing\edit_property.html", "r", encoding="utf-8") as f:
    html = f.read()

# Add button
button_html = """<input type="text" x-model="meter.pointing_to" class="w-1/3 rounded-md border-2 border-slate-300 p-2 text-sm focus:border-purple-500 mt-2" placeholder="Location">
                  <button type="button" @click="deletedMeters.push(meter.id); existingMeters.splice(index, 1)" class="text-red-500 hover:text-red-700 font-bold px-2 mt-2" title="Delete Meter">&times;</button>
"""
html = html.replace('<input type="text" x-model="meter.pointing_to" class="w-1/3 rounded-md border-2 border-slate-300 p-2 text-sm focus:border-purple-500 mt-2" placeholder="Location">', button_html)

# Add hidden input
hidden_input = """<input type="hidden" name="existing_meters_json" :value="JSON.stringify(existingMeters)">
            <input type="hidden" name="deleted_meters_json" :value="JSON.stringify(deletedMeters)">"""
html = html.replace('<input type="hidden" name="existing_meters_json" :value="JSON.stringify(existingMeters)">', hidden_input)

# Add state
state_js = """newMeters: [],
      deletedMeters: []"""
html = html.replace("newMeters: []", state_js)

with open(r"D:\Users\yeshk\Documents\ait_platform\templates\school_billing\edit_property.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Updated routes and template for meter deletion!")

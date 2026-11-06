"""Display-only meter values: omit whole-number decimals, retain fractions and data."""
import unittest
from lxml import html
from sqlalchemy import text
from test_pm_utilities_capture import UtilitiesCaptureTests

class ReadingDisplayTests(UtilitiesCaptureTests):
    def test_whole_and_fractional_values_for_water_and_electricity_do_not_change_storage(self):
        self.session.execute(text("INSERT INTO bil_meter(id,property_id,meter_number,utility_type) VALUES(98,26,'DISPLAY-E','Electrical'),(99,26,'DISPLAY-W','Water')"))
        for meter in (98,99):
            for day,value in [('2025-01-01',95639.0),('2025-02-01',0.0),('2025-03-01',123.75)]:
                self.session.execute(text('INSERT INTO bil_meter_reading(meter_id,reading_date,reading_value) VALUES(:m,:d,:v)'),dict(m=meter,d=day,v=value))
        self.session.commit();self.conn.commit();before=self.snapshot()
        response=self.client.get(self.readings_url+'?workspace=register',headers=self.headers)
        self.assertEqual(response.status_code,200,response.text)
        tree=html.fromstring(response.data)
        for meter,unit in [('98','kWh'),('99','kL')]:
            values=[cell.text_content().strip() for cell in tree.xpath('//tr[@data-reading-row][@data-meter-id="'+meter+'"]'+'/td[3]')]
            self.assertEqual(values,['123.75 '+unit,'0 '+unit,'95639 '+unit])
        self.assertEqual(before,self.snapshot())
        self.assertEqual(self.client.get('/billing/utilities/28/meter-readings?workspace=register',headers=self.headers).status_code,403)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReadingDisplayTests(name) for name in ReadingDisplayTests.__dict__ if name.startswith('test_'))

"""Build the SACE facilitator aid from unchanged workshop slides 1-31.

Run directly; does not import Flask, connect to a database or alter source slides.
"""
from pathlib import Path
from io import BytesIO
from PIL import Image
from xml.sax.saxutils import escape
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph
from reportlab.lib.utils import ImageReader

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / 'app/static/pdf/F_Guide.pdf'
# Purpose, facilitator action, participant action. Order matches the source PNGs.
NOTES = [
('Introduce the foundation session.', 'Outline the pre-test, reading problem and introduction to LITRE shown on the agenda.', 'Follow the session outline and prepare to respond to the pre-test.'),
('Elicit views on the reading crisis.', 'Read the question and the two choices without adding an answer.', 'Choose True or False.'),
('Review the displayed survey results.', 'Compare the displayed counts, 04 and 16, with the question; identify them as the results shown on this slide.', 'Compare the displayed responses with your own view.'),
('Explain the reading problem described in the presentation.', 'Summarise the links between early literacy, later learning and the systemic consequences listed.', 'Consider how these difficulties appear in your classroom.'),
('Frame the problem LITRE addresses.', 'Read the question about making blending concrete, visible and memorable.', 'Consider how a learner can experience constructing a word.'),
('Explore perceived causes of the reading crisis.', 'Read the four possible contributors shown.', 'Select the contributor you consider greatest.'),
('Introduce I Learn to Read English Using the LITRE Method.', 'Point out the vowels, consonants and blending place in the palm illustration.', 'Observe how the picture represents bringing letters together.'),
('Explain why LITRE was developed.', 'Retell the experience on the slide, distinguishing memorising a familiar story from breaking down and reconstructing words.', 'Notice the difference between recall and blending.'),
('Summarise what LITRE is.', 'Explain the visual, practical, participatory and structured features, following Story, See, Do, Blend, Read.', 'Follow the sequence and the Learn, Introduce, Teach, Repeat, Encourage wording.'),
('Introduce the practical session.', 'Outline the palm, English Family, vowels and LITRE Blending Machine activities in the displayed order.', 'Prepare to draw and use the palm.'),
('Create the palm teaching aid.', 'Demonstrate drawing the outline shown, keeping the palm and fingers clear.', 'Draw the palm.'),
('Introduce the Who is Leaving HOME? palm layout.', 'Point to HOME, the CV song and the vowel positions and paths exactly as drawn.', 'Add the displayed positions to your palm and follow the paths.'),
('Introduce vowels and consonants through the English Family.', 'Use the illustrated family story to identify the two letter groups.', 'Recognise and name the letters in each group.'),
('Make the letter flashcards.', 'Demonstrate three folds to make eight pieces per A4 sheet, then cutting and lettering as instructed.', 'Use three to four sheets and write A to Z on separate cards.'),
('Practise the first LITRE Sign Language Game.', 'Demonstrate the displayed three-hand, two-hand and one-hand word examples.', 'Practise the listed words using the demonstrated hand sequence.'),
('Practise the second LITRE Sign Language Game.', 'Demonstrate the English and Nguni examples with the meanings shown.', 'Repeat the displayed examples using the hand sequence.'),
('Introduce the vowel group and fixed positions.', 'Point to a, e, i, o, u on the palm and say the sounds using the pictured examples.', 'See the letters, say the sounds and remember their positions.'),
('Reinforce the vowel sequence through movement.', 'Demonstrate the five small forward hops, pronouncing the next vowel on each hop.', 'Carry out the displayed A, E, I, O, U activity.'),
('Practise rapid vowel recall with the number map.', 'Explain 1=A, 2=E, 3=I, 4=O, 5=U and demonstrate a call and response.', 'Pair up: A calls a number, B says its vowel, then switch roles.'),
('Explain the LITRE Blending Machine.', 'Show the palm as the meeting place, vowels in fixed positions and consonants moving in; demonstrate blend then read.', 'Follow the letters coming together and say the blended word.'),
('Introduce oral practice and classroom application.', 'Outline the t, m and p blending, word construction, classroom use, assessment and reflection agenda.', 'Prepare for the oral demonstrations and application activities.'),
('Blend t with the vowels.', 'Demonstrate ta, te, ti, to, tu using the pronunciation cues printed in the table.', 'Repeat the blends while following the palm positions.'),
('Blend m with the vowels.', 'Demonstrate ma, me, mi, mo, mu and refer to the meanings shown.', 'Repeat the blends and follow the table.'),
('Construct the six-letter word tomato.', 'Select to, ma and to as illustrated, then blend them in order.', 'Follow the selections and read to-ma-to as tomato.'),
('Blend p with the vowels.', 'Demonstrate pa, pe, pi, po, pu using the printed sound cues.', 'Repeat the blends while following the palm positions.'),
('Apply LITRE to catch-up classroom support.', 'Explain identifying difficulty, practising systematically and monitoring progress using the three sections.', 'Consider the sounds or blends requiring support and the next practice step.'),
('Plan a classroom activity.', 'Explain choosing a learner group, selecting an activity, demonstrating and assessing.', 'Plan a short activity, prepare materials and describe how you will check understanding.'),
('Review knowledge, practical demonstration and application.', 'Explain the three assessment areas and demonstrate what the palm activity involves.', 'Answer questions, demonstrate with a partner and share your classroom plan for feedback.'),
('Reflect and plan the next action.', 'Use the What did I learn?, How will I use it? and My action plan prompts.', 'Identify one activity to try, daily practice and how to monitor progress.'),
('Consolidate the workshop and classroom next steps.', 'Thank participants and recap daily palm use, blending, worksheets and progress monitoring.', 'Review your next steps, then continue to the final presentation slide.'),
('Consider pronunciation and silent letters in ISLE.', 'Read the displayed question and options; demonstrate the spoken word and invite discussion without changing the slide.', 'Say the word and discuss the displayed choices.'),
]


def build():
    assert len(NOTES) == 31
    slides = [ROOT / f'app/static/sace_slides/{i}.png' for i in range(1, 32)]
    for slide in slides:
        if not slide.is_file():
            raise FileNotFoundError(slide)
    width, height = landscape(A4)
    pdf = canvas.Canvas(str(OUTPUT), pagesize=(width, height), pageCompression=1, invariant=1)
    pdf.setTitle('Facilitator Manual - I Learn to Read English Using the LITRE Method')
    pdf.setAuthor('AIT')
    style = ParagraphStyle('note', fontName='Helvetica', fontSize=10, leading=13, textColor='#1e293b')
    for number, (slide, notes) in enumerate(zip(slides, NOTES), 1):
        pdf.setFillColorRGB(0.19, 0.18, 0.51)
        pdf.rect(0, height - 7, width, 7, fill=1, stroke=0)
        pdf.setFont('Helvetica-Bold', 15)
        pdf.drawString(28, height - 30, 'Facilitator Manual')
        pdf.setFont('Helvetica', 9)
        pdf.drawRightString(width - 28, height - 29, f'Workshop slide {number} of 31')
        pdf.drawString(28, height - 46, 'I Learn to Read English Using the LITRE Method')
        # Embed at original dimensions with print-quality JPEG encoding; no crop or content edits.
        encoded = BytesIO()
        with Image.open(slide) as source:
            source.convert('RGB').save(encoded, format='JPEG', quality=90, optimize=True)
        encoded.seek(0)
        image = ImageReader(encoded)
        iw, ih = image.getSize()
        scale = min((width - 56) / iw, 390 / ih)
        dw, dh = iw * scale, ih * scale
        pdf.drawImage(image, (width - dw) / 2, height - 60 - dh, width=dw, height=dh, mask='auto')
        y = 126
        for label, note in zip(('Purpose', 'Facilitator', 'Participants'), notes):
            paragraph = Paragraph(f'<b>{label}:</b> {escape(note)}', style)
            _, ph = paragraph.wrap(width - 56, 100)
            paragraph.drawOn(pdf, 28, y - ph)
            y -= ph + 5
        if y < 28:
            raise ValueError(f'Notes overflow on slide {number}')
        pdf.setFont('Helvetica', 8)
        pdf.setFillColorRGB(0.4, 0.4, 0.4)
        pdf.drawString(28, 16, 'Workshop presentation: slides 1-31. Online interactions: steps 32-35 (outside this manual).')
        pdf.showPage()
    pdf.save()
    print(f'Created {OUTPUT} ({len(NOTES)} pages)')


if __name__ == '__main__':
    build()
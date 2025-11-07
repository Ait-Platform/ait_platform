import os
import time
import zipfile
import numpy as np
from pydub import AudioSegment
import pyttsx3

# -----------------------------
# CONFIG
# -----------------------------

OUTPUT_DIR = os.path.join("static", "audio", "reading")
BG_MUSIC_FILE = os.path.join(OUTPUT_DIR, "background_bed.mp3")  # you provide this
ZIP_PATH = os.path.join(OUTPUT_DIR, "reading_about_audio_pack.zip")

TARGET_LANG_CODES = [
    ("en", "English"),
    ("af", "Afrikaans"),
    ("zu", "Zulu"),
    ("xh", "Xhosa"),
    ("hi", "Hindi"),
    ("zh", "Mandarin"),
    ("es", "Spanish"),
    ("fr", "French"),
    ("it", "Italian"),
    ("ru", "Russian"),
]

# LUFS-ish target loudness (approx -16 LUFS).
# We'll just normalize peak/RMS roughly since full LUFS metering needs extra deps.
TARGET_DBFS = -16.0

# 1 second silence between alternating voices
SPLIT_PAUSE_MS = 1000

# How much lower (dB) background bed should sit under voices
BG_MUSIC_ATTENUATION_DB = -22  # softer than speech

# -----------------------------
# TEXT SCRIPTS PER LANGUAGE
# Each script is split into chunks.
# We'll alternate male/female voice per chunk.
# -----------------------------

SCRIPTS = {
    "en": [
        # chunk 0 - male
        "Welcome to the Reading Programme. This course has short video lessons. Each lesson focuses on sounds, word building, and oral practice. You can pause, go back, and replay as many times as you need.",
        # chunk 1 - female
        "The lessons tell a story while helping you build your reading skills. This programme uses a new method that moves away from traditional phonics. You don’t need to know the rules of English before you start. You’ll learn as you go.",
        # chunk 2 - male
        "By the end, you’ll be able to read and spell English words with confidence, often within just one hour of focused study. Remember to practise speaking out loud. That is how you build clear pronunciation.",
        # chunk 3 - female
        "Here’s how it works. Your access window is five days. We only start counting once you press play on your first lesson, not when you pay. You can watch any lesson, in any order, and replay as often as you like. We track practice, not speed.",
        # chunk 4 - male
        "You’ll see a live tracker that shows how many lessons you’ve started, how many times you repeated them, and how much time you have left in your five-day window. At the end, you’ll receive a certificate of completion with your name and practice record. You can always enrol again.",
        # chunk 5 - female
        "The lessons also include guidance and subtitles in different languages. You can understand the instructions in your own language, and you can switch language during the course without losing progress.",
    ],

    "af": [
        "Welkom by die Leesprogram. Hierdie kursus het kort video-lesse. Elke les fokus op klanke, woordbou en mondelinge oefening. Jy kan stop, teruggaan en weer speel soveel keer as wat jy wil.",
        "Die lesse vertel ’n storie terwyl jy jou leesvaardigheid verbeter. Hierdie program gebruik ’n nuwe metode, weg van tradisionele fonetika. Jy hoef nie eers Engels se reëls te ken voordat jy begin nie. Jy leer terwyl jy aangaan.",
        "Teen die einde sal jy woorde met selfvertroue kan lees en spel, dikwels binne ’n uur se gefokusde studie. Onthou om hardop te oefen. Dit is hoe jy jou uitspraak verbeter.",
        "So werk dit. Jou toegang is vir vyf dae. Ons begin eers tel wanneer jy jou eerste les speel, nie wanneer jy betaal nie. Jy kan enige les kyk in enige volgorde, en dit soveel keer herhaal as wat jy wil. Ons hou oefening dop, nie spoed nie.",
        "Jy sal ’n vorderingsmeter sien wat wys hoeveel lesse jy begin het, hoe gereeld jy dit herhaal het, en hoeveel tyd oorbly in die vyf dae. Aan die einde kry jy ’n sertifikaat van voltooiing met jou naam en jou oefenrekord. Jy kan weer inskryf wanneer jy wil.",
        "Die lesse sluit verduidelikings en onderskrifte in jou eie taal in. Jy kan die instruksies in jou moedertaal verstaan en jy kan die taal tydens die kursus verander sonder om jou vordering te verloor.",
    ],

    "zu": [
        "Siyakwamukela kuhlelo lokufunda. Lolu hlelo lunezifundo ezimfushane zamavidiyo. Isifundo ngasinye sigxile emsindweni, ekwakheni amagama, nasekuqeqesheni ukukhuluma. Ungaphinda noma yimuphi umzuzu, noma uqalise phansi, noma nini.",
        "Izifundo zilandisa indaba ngenkathi zikusiza ukuthi ufunde ukufunda ngesiNgisi. Lolu hlelo lusebenzisa indlela entsha, hhayi indlela yakudala yephonics. Awudingi ukwazi imithetho yolimi lwesiNgisi ngaphambi kokuqala. Ufunda njengoba uhamba.",
        "Ekupheleni, uzokwazi ukufunda nokubhala amagama esiNgisi ngokuzethemba, kwesinye isikhathi ngaphansi kwehora elilodwa. Khumbula ukuprakthiza ngokukhuluma ngezwi eliphakeme. Lokho kukusiza ukuphimisa kahle.",
        "Nansi indlela esebenza ngayo. Isikhathi sakho sokufinyelela yizinsuku ezinhlanu. Siqala ukubala kuphela uma usucofe u-Play esifundweni sokuqala, hhayi ngesikhathi sokukhokha. Ungabuka noma yiliphi isifundo nganoma yiliphi isikhathi, futhi uphinde izikhathi eziningi. Sibheka ukuzilolongela kwakho, hhayi ijubane.",
        "Uzobona umkhondo ophilayo okhombisa ukuthi zingaki izifundo osuqalile, zingaki izikhathi oziphindile, nokuthi isikhathi esingakanani esisele kulezi zinsuku ezinhlanu. Ekugcineni, uzothola isitifiketi sokuqeda esinegama lakho kanye nerekhodi lakho lokuzilolonga. Ungabhalisa futhi noma nini.",
        "Izifundo nazo zinokuchazwa ngolimi lwakho. Lokho kukuvumela uqonde imiyalelo ngolimi lwasekhaya, futhi ungashintsha ulimi ngesikhathi sohlelo ngaphandle kokulahlekelwa inqubekela phambili.",
    ],

    "xh": [
        "Wamkelekile kwinkqubo yokufunda. Le nkqubo inezifundo ezimfutshane zevidiyo. Isifundo ngasinye sigxile kwisandi, ekwakheni amagama, nasekuziqheliseni ukuthetha. Ungayiphinda ividiyo nanini na, kwaye uqale kwakhona.",
        "Izifundo zibalisa ibali ngexesha elinye zikunceda uphucule izakhono zakho zokufunda ngesiNgesi. Le nkqubo isebenzisa indlela entsha, hayi i-phonics yakudala. Akufuneki wazi yonke imithetho yesiNgesi ngaphambi kokuqala. Uya kufunda kancinci kancinci njengoko uqhubeka.",
        "Ekugqibeleni, uya kukwazi ukufunda nokupela amagama esiNgesi ngokuzithemba, ngamanye amaxesha ngaphantsi kweyure enye. Khumbula ukuziqhelisa ngokuthetha ngokuvakalayo. Oko kukunceda kuphucule ukuthetha kwakho.",
        "Nantsi indlela esebenza ngayo. Ixesha lakho lokufikelela ziintsuku ezintlanu. Siqala ukubala kuphela xa ucofa u-Play kwisifundo sokuqala, hayi xa uhlawula. Ungabona naliphi na isifundo nangaliphi na ixesha, kwaye uphinde kaninzi. Sigcina umkhondo womsebenzi wakho wokuzilolonga, hayi isantya.",
        "Uya kubona umkhondo wokwenyani obonisa ukuba zingaphi izifundo oziphumezileyo, kangaphi uziphindileyo, kunye nexesha elishiyekileyo kula maentsuku mahlanu. Ekugqibeleni, ufumana isatifikethi sokugqiba esinegama lakho kunye nerekhodi lakho lokuziqhelisa. Ungabhalisa kwakhona nanini na.",
        "Izifundo zikwabonelela ngenkcazo ngolwimi lwakho lwasemakhaya. Oko kukunceda uqonde imiyalelo lula, kwaye unokutshintsha ulwimi ngexesha lenkqubo ngaphandle kokulahlekelwa yinkqubela.",
    ],

    "hi": [
        "रीडिंग प्रोग्राम में आपका स्वागत है. यह कोर्स छोटे वीडियो लेसन से बना है. हर लेसन ध्वनियों, शब्द बनाने और बोल कर अभ्यास करने पर ध्यान देता है. आप किसी भी समय रोक सकते हैं, पीछे जा सकते हैं और फिर से चला सकते हैं.",
        "ये लेसन एक कहानी की तरह चलते हैं और आपको अंग्रेज़ी पढ़ने की क्षमता बनाने में मदद करते हैं. यह एक नई विधि है, पुराने फॉनिक्स तरीके जैसी नहीं. आपको पहले से अंग्रेज़ी के नियम जानने की ज़रूरत नहीं है. आप धीरे-धीरे सीखते जाएंगे.",
        "आख़िर में आप आत्मविश्वास के साथ अंग्रेज़ी शब्द पढ़ और स्पेल कर पाएंगे, कई बार सिर्फ़ एक घंटे के अभ्यास में. ज़ोर से बोल कर अभ्यास करें. बोल कर अभ्यास से उच्चारण बेहतर होता है.",
        "ये है तरीका. आपके पास पाँच दिन की पहुँच होती है. गिनती तभी शुरू होती है जब आप पहला लेसन चलाते हैं, न कि जब आप भुगतान करते हैं. आप किसी भी लेसन को किसी भी क्रम में देख सकते हैं, और जितनी बार चाहें दोहरा सकते हैं. हम स्पीड नहीं, प्रैक्टिस देखते हैं.",
        "आपको एक ट्रैकर दिखाई देगा जो दिखाता है कि आपने कितने लेसन शुरू किए, उन्हें कितनी बार दोहराया, और पाँच दिनों में कितना समय बचा है. अंत में आपको एक कम्प्लीशन सर्टिफिकेट मिलेगा जिसमें आपका नाम और आपका अभ्यास रिकॉर्ड होगा. आप कभी भी फिर से नामांकन कर सकते हैं.",
        "लेसन में आपकी भाषा में निर्देश और सबटाइटल भी होंगे. इससे आप आसानी से समझ पाएंगे, और आप कोर्स के दौरान भाषा बदल सकते हैं बिना प्रोग्रेस खोए.",
    ],

    "zh": [
        "欢迎来到阅读课程。本课程包含简短的视频课程。每节课都专注于发音、拼词和口语练习。你可以随时暂停，倒回，反复观看。",
        "课程以故事的方式帮助你建立英文阅读能力。这是一种全新的方法，而不是传统的自然拼读。开始之前你不需要了解英语的语法或规则。你会一边学一边掌握。",
        "在课程结束时，你会有信心阅读和拼写英文单词，很多学习者只需要一小时的专注练习。请记得大声练习发音，这是提高口语清晰度的关键。",
        "运作方式如下。你拥有五天的学习时间。计时会在你播放第一节课时才开始，而不是在付款时开始。你可以按照任何顺序观看任意一节课，并且可以重复多次。我们关注的是练习，而不是速度。",
        "你会看到一个进度追踪器：它会显示你开始了多少节课，每节课重复了多少次，以及五天时间里还剩多少。最后，你会得到一份完成证书，上面会标有你的名字和你的练习记录。你可以随时重新报名，重新开始。",
        "课程还提供多语言的指引和字幕，帮助你用母语理解教学内容。你可以在课程中切换语言，而不会丢失进度。",
    ],

    "es": [
        "Bienvenido al Programa de Lectura. Este curso tiene lecciones cortas en video. Cada lección se enfoca en sonidos, construcción de palabras y práctica oral. Puedes pausar, retroceder y repetir tantas veces como necesites.",
        "Las lecciones cuentan una historia mientras desarrollas tu habilidad para leer en inglés. Este programa usa un método nuevo, diferente a la fonética tradicional. No necesitas conocer las reglas del inglés antes de empezar. Aprendes paso a paso.",
        "Al final, podrás leer y deletrear palabras en inglés con confianza, muchas veces en sólo una hora de práctica enfocada. Recuerda practicar en voz alta. Eso es lo que mejora tu pronunciación.",
        "Así es como funciona. Tu acceso dura cinco días. El conteo empieza sólo cuando reproduces tu primera lección, no cuando pagas. Puedes ver cualquier lección, en cualquier orden, y repetirla todas las veces que quieras. Medimos práctica, no velocidad.",
        "Verás un registro en vivo que te muestra cuántas lecciones iniciaste, cuántas veces repetiste cada una y cuánto tiempo te queda dentro de los cinco días. Al final recibirás un certificado de finalización con tu nombre y tu historial de práctica. Siempre puedes inscribirte de nuevo.",
        "Las lecciones también incluyen guías y subtítulos en diferentes idiomas, para que entiendas las instrucciones en tu propio idioma. Puedes cambiar de idioma durante el curso sin perder tu progreso.",
    ],

    "fr": [
        "Bienvenue dans le Programme de Lecture. Ce cours propose de courtes leçons vidéo. Chaque leçon se concentre sur les sons, la construction des mots et la pratique orale. Vous pouvez faire pause, revenir en arrière et revoir autant de fois que nécessaire.",
        "Les leçons racontent une histoire tout en vous aidant à développer vos compétences de lecture en anglais. Le programme utilise une méthode nouvelle, différente de la phonétique traditionnelle. Vous n’avez pas besoin de connaître toutes les règles de l’anglais avant de commencer. Vous apprenez au fur et à mesure.",
        "À la fin, vous pourrez lire et épeler des mots en anglais avec confiance, parfois après seulement une heure d’entraînement ciblé. N’oubliez pas de pratiquer à voix haute. C’est comme ça que vous améliorez votre prononciation.",
        "Voici comment cela fonctionne. Vous avez cinq jours d’accès. Le décompte commence uniquement lorsque vous lancez votre toute première leçon, pas quand vous payez. Vous pouvez regarder n’importe quelle leçon, dans l’ordre que vous voulez, et la revoir autant de fois que vous le souhaitez. Nous mesurons la pratique, pas la vitesse.",
        "Vous verrez un suivi en direct qui montre combien de leçons vous avez commencées, combien de fois vous les avez répétées et combien de temps il vous reste dans votre fenêtre de cinq jours. À la fin, vous recevrez un certificat d’achèvement avec votre nom et votre historique de pratique. Vous pouvez toujours vous réinscrire.",
        "Les leçons incluent aussi des explications et des sous-titres dans différentes langues, pour que vous puissiez comprendre les consignes dans votre langue. Vous pouvez changer de langue pendant le cours sans perdre votre progression.",
    ],

    "it": [
        "Benvenuto al Programma di Lettura. Questo corso è fatto di brevi lezioni video. Ogni lezione si concentra sui suoni, sulla costruzione delle parole e sulla pratica orale. Puoi mettere in pausa, tornare indietro e riascoltare tutte le volte che vuoi.",
        "Le lezioni raccontano una storia mentre sviluppi la tua capacità di leggere in inglese. Il programma usa un metodo nuovo, diverso dalla fonetica tradizionale. Non devi conoscere tutte le regole dell’inglese prima di iniziare. Impari man mano che vai avanti.",
        "Alla fine sarai in grado di leggere e fare lo spelling delle parole inglesi con sicurezza, spesso dopo solo un’ora di studio concentrato. Ricorda di esercitarti ad alta voce: è così che migliori la pronuncia.",
        "Ecco come funziona. Hai cinque giorni di accesso. Il conteggio inizia solo quando fai partire la tua prima lezione, non quando paghi. Puoi guardare qualsiasi lezione, in qualsiasi ordine, e rivederla quante volte vuoi. Misuriamo la pratica, non la velocità.",
        "Vedrai un indicatore in tempo reale che mostra quante lezioni hai iniziato, quante volte le hai ripetute e quanto tempo ti rimane nei cinque giorni. Alla fine riceverai un certificato di completamento con il tuo nome e il tuo registro di pratica. Puoi sempre iscriverti di nuovo.",
        "Le lezioni includono anche spiegazioni e sottotitoli in diverse lingue, così puoi capire le istruzioni nella tua lingua. Puoi cambiare lingua durante il corso senza perdere i tuoi progressi.",
    ],

    "ru": [
        "Добро пожаловать в Программу Чтения. Этот курс состоит из коротких видеоуроков. Каждый урок сосредоточен на звуках, построении слов и устной практике. Вы можете ставить на паузу, перематывать назад и пересматривать столько раз, сколько нужно.",
        "Уроки рассказывают историю и одновременно помогают вам развивать навык чтения на английском языке. Эта программа использует новый метод, а не традиционную фонетику. Вам не нужно заранее знать правила английского. Вы будете учиться постепенно, по ходу.",
        "К концу курса вы сможете уверенно читать и писать английские слова — иногда уже после одного часа целенаправленной практики. Не забывайте тренироваться вслух. Это помогает улучшить произношение.",
        "Как это работает. У вас есть доступ на пять дней. Отсчёт начинается только тогда, когда вы запускаете свой первый урок, а не в момент оплаты. Вы можете смотреть любой урок в любом порядке и пересматривать его сколько угодно раз. Мы оцениваем практику, а не скорость.",
        "Вы увидите индикатор прогресса: сколько уроков вы начали, сколько раз вы их повторили и сколько времени осталось в пределах ваших пяти дней. В конце вы получите сертификат о завершении с вашим именем и историей практики. Вы всегда можете записаться снова.",
        "Уроки также содержат пояснения и субтитры на разных языках, чтобы вы могли понимать инструкции на своём родном языке. Вы можете переключать язык в любое время, не теряя свой прогресс.",
    ],
}

# -----------------------------
# UTILITIES
# -----------------------------

def init_tts_engine():
    engine = pyttsx3.init()
    # we'll adjust rate and volume, voice gender per chunk
    return engine

def pick_voice(engine, gender="male"):
    """
    Try to pick a voice ID. If we can't match gender,
    just return the first available voice.
    """
    voices = engine.getProperty("voices")

    # Try a gender-ish guess
    if gender == "male":
        preferred_terms = ["male", "man", "guy", "baritone"]
    else:
        preferred_terms = ["female", "woman", "girl", "alto", "soprano"]

    # print voices for debug
    print(f"[voice-scan] Looking for {gender} voice...")
    for v in voices:
        desc = f"{v.id} || {v.name} || {getattr(v, 'gender', '')}".lower()
        print("   ", desc)
        if any(term in desc for term in preferred_terms):
            print(f"[voice-pick] chose {v.id}")
            return v.id

    # fallback to first voice if we didn't match
    if voices:
        print(f"[voice-pick:fallback] chose {voices[0].id}")
        return voices[0].id

    print("[voice-pick:fail] no voices found at all")
    return None

import uuid

import uuid
import time
import pyttsx3
from pydub import AudioSegment
import os

def tts_to_segment(text, gender, rate_reduction=25):
    """
    Generate spoken audio for `text` using pyttsx3 in a fresh engine.
    We spin up a new engine each time to avoid SAPI deadlocks when
    switching voices.
    """
    tmp_wav = f"_tmp_tts_{uuid.uuid4().hex}.wav"

    # fresh engine every call
    engine = pyttsx3.init()

    # pick voice again for this gender
    voice_id = pick_voice(engine, gender)
    if voice_id is None:
        raise RuntimeError(f"No voice available for gender={gender}")

    engine.setProperty("voice", voice_id)

    base_rate = engine.getProperty("rate")
    engine.setProperty("rate", max(100, base_rate - rate_reduction))

    # synth -> wav
    engine.save_to_file(text, tmp_wav)
    engine.runAndWait()
    engine.stop()

    # wait for file to land and not be empty
    for _ in range(40):  # give it a little longer now
        if os.path.exists(tmp_wav) and os.path.getsize(tmp_wav) > 0:
            break
        time.sleep(0.1)

    if not os.path.exists(tmp_wav) or os.path.getsize(tmp_wav) == 0:
        raise RuntimeError(f"TTS output file {tmp_wav} was not created or is empty")

    seg = AudioSegment.from_wav(tmp_wav)
    os.remove(tmp_wav)
    return seg


def normalize_to_target_dbfs(seg: AudioSegment, target_dbfs: float) -> AudioSegment:
    change_needed = target_dbfs - seg.dBFS
    return seg.apply_gain(change_needed)


def loop_bed_to_length(bed: AudioSegment, length_ms: int) -> AudioSegment:
    if len(bed) == 0:
        return bed
    loops = int(np.ceil(length_ms / len(bed)))
    out = bed * loops
    return out[:length_ms]


def mix_with_bed(voice_seg: AudioSegment, bed: AudioSegment) -> AudioSegment:
    """
    Lower background bed volume, loop to match length, then overlay.
    """
    if bed is None or len(bed) == 0:
        return voice_seg

    # attenuate bg
    bg_quiet = bed + BG_MUSIC_ATTENUATION_DB
    bg_looped = loop_bed_to_length(bg_quiet, len(voice_seg))
    mixed = bg_looped.overlay(voice_seg)
    return mixed


# -----------------------------
# MAIN GENERATION
# -----------------------------

def build_language_track(lang_code, paragraphs, bg_music_seg):
    print(f"[+] Generating language: {lang_code}")

    full_track = AudioSegment.silent(duration=0)

    for idx, chunk in enumerate(paragraphs):
        gender = "male" if idx % 2 == 0 else "female"
        print(f"   [chunk {idx}] gender={gender} len(chars)={len(chunk)}")

        spoken = tts_to_segment(chunk, gender)

        # build pause that matches spoken format
        pause_seg = AudioSegment.silent(duration=SPLIT_PAUSE_MS)
        pause_seg = pause_seg.set_frame_rate(spoken.frame_rate) \
                             .set_channels(spoken.channels) \
                             .set_sample_width(spoken.sample_width)

        chunk_with_pause = spoken + pause_seg
        full_track += chunk_with_pause

    # normalize voice track
    full_track = normalize_to_target_dbfs(full_track, TARGET_DBFS)

    # only mix bed if bg_music_seg is non-empty
    if bg_music_seg is not None and len(bg_music_seg) > 0:
        final_mix = mix_with_bed(full_track, bg_music_seg)
    else:
        final_mix = full_track

    # normalize final
    final_mix = normalize_to_target_dbfs(final_mix, TARGET_DBFS)

    return final_mix


def main():
    # ensure output dir
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # load background bed
    # load background bed
    if os.path.exists(BG_MUSIC_FILE) and os.path.getsize(BG_MUSIC_FILE) > 0:
        bg_music_seg = AudioSegment.from_file(BG_MUSIC_FILE)
    else:
        print("[!] WARNING: No usable background bed found. Voices will be dry.")
        bg_music_seg = AudioSegment.silent(duration=0)


    generated_files = []

    for lang_code, _lang_name in TARGET_LANG_CODES:
        if lang_code not in SCRIPTS:
            print(f"[!] No script for {lang_code}, skipping.")
            continue

        paragraphs = SCRIPTS[lang_code]

        final_mix = build_language_track(lang_code, paragraphs, bg_music_seg)

        outfile = os.path.join(OUTPUT_DIR, f"about_{lang_code}.mp3")
        final_mix.export(outfile, format="mp3", bitrate="192k")
        generated_files.append(outfile)
        print(f"[+] Wrote {outfile}")

    # zip them
    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in generated_files:
            arcname = os.path.basename(f)
            zf.write(f, arcname=arcname)

    print("[✓] Done.")
    print(f"[✓] Audio pack at {ZIP_PATH}")


if __name__ == "__main__":
    main()

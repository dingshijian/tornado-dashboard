import os
import re
import googlemaps
import speech_recognition as sr
import pyttsx3


def speak(text, engine):
    engine.say(text)
    engine.runAndWait()


def listen(prompt, recognizer, engine):
    with sr.Microphone() as source:
        print(prompt)
        speak(prompt, engine)
        audio = recognizer.listen(source)
    try:
        text = recognizer.recognize_google(audio)
        print(f"You said: {text}")
        return text
    except sr.UnknownValueError:
        speak("I didn't catch that. Please repeat.", engine)
        return listen(prompt, recognizer, engine)


def get_directions(start, destination, gmaps):
    directions = gmaps.directions(start, destination, mode="driving")
    if not directions:
        return []
    steps = directions[0]["legs"][0]["steps"]
    cleaned = []
    for step in steps:
        instructions = re.sub("<[^<]+?>", "", step.get("html_instructions", ""))
        distance = step.get("distance", {}).get("text", "")
        cleaned.append(f"{instructions} for {distance}")
    return cleaned


def main():
    api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("Missing GOOGLE_MAPS_API_KEY environment variable")
        return
    gmaps = googlemaps.Client(key=api_key)
    recognizer = sr.Recognizer()
    engine = pyttsx3.init()

    destination = listen("Where would you like to go?", recognizer, engine)
    start = listen("Where are you starting from?", recognizer, engine)

    directions = get_directions(start, destination, gmaps)
    if not directions:
        speak("I could not find directions", engine)
        return

    for step in directions:
        print(step)
        speak(step, engine)

    speak("You have arrived at your destination", engine)


if __name__ == "__main__":
    main()

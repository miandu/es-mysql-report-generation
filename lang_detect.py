#!/usr/bin/python3
import json, argparse,utils,os,sys
sys.path.append(os.path.join(os.path.dirname(__file__), "libs"))
from progress.bar import Bar
from langdetect import detect_langs

def detect_languages(cf,message):
    detection = detect_langs(message)
    for i in detection:
        yield str(i).split(':')

def get_most_probable_lang(cf,languages):
    if len(languages)==0:
        return 'uncertain'
    else:
        if float(languages[0][1])>=cf.minimum_lang_probability:
            return languages[0][0]
        else:
            return 'uncertain'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Language detection for message.') 
    parser.add_argument('config', type=str, help='An config file for language detection')
    parser.add_argument('message', type=str, help='Message for language detection')

    args = parser.parse_args()
    config = __import__(args.config)
    cf =config.Config() 
    
    print(get_most_probable_lang(cf,list(detect_languages(cf,args.message))))
    


# Naive sentiment analysis script that uses flair model. 
# It has also google translator API since I was trying to do
# the analysis traduced sentence. 
# Still working on it
# 
# ATR the moment the template is ready but nothing has been done yet (in terms of analysis). 
# I will continue when I have time...  #



import os
import flair
import numpy as np


if __name__ == '__main__':
    print('inizio test...')

    sentiment_model = flair.models.TextClassifier.load('en-sentiment')
    a=1

    sentence = flair.data.Sentence('im happy')
    sentiment_model.predict(sentence)
    probability = sentence.labels[0].score  # numerical value 0-1
    sentiment = sentence.labels[0].value  # 'POSITIVE' or 'NEGATIVE'
    print(probability)
    print(sentiment)



    'test google translator api '
    from googletrans import Translator

    translator = Translator()

    frasi = ['il ragazzo piange' , 'le montagne sono piene di neve', 'il diavolo veste prada', 'il terremoto ha distrutto la citta', 'incendio non ha distrutto nemmeno una casa' ]

    for frase in frasi:

        translator.raise_Exception = True

        print(frase, ' --> ', translator.translate(frase, src='it').text)

        # translations = translator.translate('The quick brown fox', dest='it')
        # for translation in translations:
        #     print(translation.origin, ' -> ', translation.text)

        sentence = flair.data.Sentence(frase)
        sentiment_model.predict(sentence)
        probability = sentence.labels[0].score  # numerical value 0-1
        sentiment = sentence.labels[0].value  # 'POSITIVE' or 'NEGATIVE'
        print('\t prob --> ', probability)
        print('\t sent --> ', sentiment)








    print('...end.')
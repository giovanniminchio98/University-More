
# This is a template program intented to be used for sentiment analysis of cryptos prices
# It needs the API from twitter in order to get new tweets about the crypto, that will be analyzed
# Till  #

'imports'
from http.client import responses
import os
import pandas as pd
import numpy as np
import re
import flair

# to interact with twitter api
import requests

BEARER_TOKEN='ghesboro accettatemi'

#functions
def get_data(tweet):
    data = {
        'id': tweet['id_str'],
        'created_at': tweet['created_at'],
        'text': tweet['full_text']
    }
    return data

if __name__=='_-main__':

    # example of request for twitter tweet info request
    params = {'q': 'tesla',
          'tweet_mode': 'extended'}
    requests.get(
        'https://api.twitter.com/1.1/search/tweets.json',
        params=params,
        headers={'authorization': 'Bearer '+BEARER_TOKEN}
    )

    # better params with more info
    # example get only english tweet
    params = {
        'q': 'tesla',
        'tweet_mode': 'extended',
        'lang': 'en',
        'count': '100' # minimum number of tweet to return
    }

    # Building Our Dataset
    # Once we have our API request setup, we can begin 
    # running it to populate our dataset.
    df = pd.DataFrame()
    for tweet in responses.json()['statuses']:
        row = get_data(tweet)
        df = df.append(row, ignore_index=True)

    print(df.heaf())

    # Sentiment Analysis
    # We will be using a pre-trained sentiment analysis 
    # model from the flair library. As far as 
    # pre-trained models go, this is one of the most powerful.

    # This model splits the text into character-level tokens
    #  and uses the DistilBERT model to make predictions.

    # The advantage of working at the character-level 
    # (as opposed to word-level) is that words that the 
    # network has never seen before can still be assigned 
    # a sentiment.

    # Flair
    # To use the flair model, we first need to 
    # import the library with pip install flair. 
    # Once installed, we import and initialize the model like so:

    import flair
    sentiment_model = flair.models.TextClassifier.load('en-sentiment')

    # All we need to do now is tokenize our text by passing 
    # it through flair.data.Sentence(<TEXT HERE>) and calling the .
    # predict method on our model. Putting those together, we get:

    TEXT = 'prova prova'

    sentence = flair.data.Sentence(TEXT)
    sentiment_model.predict(sentence)

    # Analyzing Tesla Tweets
    # Most of our tweets are very messy. 
    # Cleaning text data is fundamental, although we will just 
    # do the bare minimum in this example.

    whitespace = re.compile(r"\s+")
    web_address = re.compile(r"(?i)http(s):\/\/[a-z0-9.~_\-\/]+")
    tesla = re.compile(r"(?i)@Tesla(?=\b)")
    user = re.compile(r"(?i)@[a-z0-9_]+")

    # we then use the sub method to replace anything matching
    tweet = whitespace.sub(' ', tweet)
    tweet = web_address.sub('', tweet)
    tweet = tesla.sub('Tesla', tweet)
    tweet = user.sub('', tweet)

    # Now we have our clean(ish) tweet — we can tokenize it by 
    # converting it into a sentence object, and then predict the sentiment:

    sentence = flair.data.Sentence(tweet)
    sentiment_model.predict(sentence)

    # Finally, we extract our predictions and add them to our tweets dataframe.
    #  We can access the label object (the prediction) by typing sentence.labels[0]. 
    # With this, we call score to get our confidence/probability score, 
    # and value for the POSITIVE/NEGATIVE prediction:

    probability = sentence.labels[0].score  # numerical value 0-1
    sentiment = sentence.labels[0].value  # 'POSITIVE' or 'NEGATIVE'

    # We can append the probability and sentiment to lists
    #  which we then merge with our tweets dataframe. 
    # Putting all of these parts together will give us:

    # we will append probability and sentiment preds later
    probs = []
    sentiments = []

    # use regex expressions (in clean function) to clean tweets
    tweets['text'] = tweets['text'].apply(clean)

    for tweet in tweets['text'].to_list():
        # make prediction
        sentence = flair.data.Sentence(tweet)
        sentiment_model.predict(sentence)
        # extract sentiment prediction
        probs.append(sentence.labels[0].score)  # numerical score 0-1
        sentiments.append(sentence.labels[0].value)  # 'POSITIVE' or 'NEGATIVE'

    # add probability and sentiment predictions to tweets dataframe
    tweets['probability'] = probs
    tweets['sentiment'] = sentiments

    print('madre')
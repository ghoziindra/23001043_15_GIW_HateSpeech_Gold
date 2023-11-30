from flask import Flask, request
from flasgger import Swagger, swag_from
from werkzeug.utils import secure_filename
import pandas as pd
import regex as re
from io import StringIO
import sqlite3


app = Flask(__name__)

app.config['SWAGGER'] = {
    'title': 'API Documentation for Data Cleansing',
    'description': 'Dokumentasi API untuk membersihkan data',
    'swagger_ui': True,
    'specs_route': '/docs/'
}

def insert_cleaned_tweet(tweets):
    try:
        print(tweets)
        sqliteConnection = sqlite3.connect('clean_word_database.db')
        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        
        sqlite_create_table_query = """CREATE TABLE IF NOT EXISTS
        cleaned_data (cleaned_tweet text)"""
        sqlite_truncate_table_query = "TRUNCATE cleaned_data;"
        
        formatted_strings = [f'("{sentence.strip()}")' for sentence in tweets]
        result_string = ', '.join(formatted_strings)


        sqlite_insert_query = f"""INSERT INTO cleaned_data
                            (cleaned_tweet) 
                            VALUES 
                            {result_string}"""

        cursor.execute(sqlite_truncate_table_query)
        cursor.execute(sqlite_create_table_query)
        cursor.execute(sqlite_insert_query)
        sqliteConnection.commit()
        print("Record inserted successfully into SqliteDb_developers table ", cursor.rowcount)
        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert data into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

swagger = Swagger(app)

@swag_from('./text_processing_file.yml', methods=['POST'])
@app.route('/upload-file', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        # Read data.csv
        file_contents = StringIO(file.stream.read().decode('latin-1'))
        data_df = pd.read_csv(file_contents)
        abusive_df = pd.read_csv('./data/abusive.csv')
        
        abusive_words = abusive_df['ABUSIVE'].tolist()
        data_df['Tweet'] = data_df['Tweet'].apply(lambda x: re.sub(r'[^\w\s]', '', x))

        for word in abusive_words:
            data_df['Tweet'] = data_df['Tweet'].str.replace(fr"{word}", '*'*len(word), case=False)

        # Read the dictionary of made-up words
        made_up_words_df = pd.read_csv('./data/new_kamusalay.csv', header=None, names=['Original', 'Replacement'], encoding='latin-1')

        # Replace made-up words
        for index, row in made_up_words_df.iterrows():
            data_df['Tweet'] = data_df['Tweet'].str.replace(fr'\b{row["Original"]}\b', row["Replacement"])

        tweets = data_df['Tweet']
        insert_cleaned_tweet(tweets.to_list())
        return tweets.to_json(orient='records')
    else:
        return 'No file uploaded', 400
    
if __name__ == '__main__':
    app.run()
# Movies_ETL_Challenge
The streaming company Amazing Prime needs to gather data from both Wikipedia and Kaggle, for a hackathon. To do this, we will follow the ETL process: extract the Wikipedia and Kaggle data from their respective files, transform the datasets by cleaning them up and joining them together, and load the cleaned dataset into a SQL database.

# 1. Write an ETL Function to Read Three Data Files
Using Python, Pandas, the ETL process, and code refactoring, write a function that reads in the three data files and creates three separate DataFrames.
```
def etl():

    kaggle_metadata = pd.read_csv(f'{file_dir}/movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}/ratings.csv')
    
    with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
        
    wiki_movies_df = pd.DataFrame(wiki_movies_raw)
    
    return wiki_movies_df, kaggle_metadata, ratings
    
file_dir = "C:/Users/alelo/Documents/BootCamp Tec/Module 8_ETL/Movies_ETL_Challenge"

wiki_file = f'{file_dir}/wikipedia_movies.json'

kaggle_file = f'{file_dir}/movies_metadata.csv'

ratings_file = f'{file_dir}/ratings.csv'

wiki_file, kaggle_file, ratings_file = etl()
    
 ```   
### Wiki_movies DataFrame

![image](https://user-images.githubusercontent.com/43974872/197236109-9cbf3fdd-10c7-4749-a1c0-5a12170e1ed7.png)

### Kaggle_metadata DataFrame

![image](https://user-images.githubusercontent.com/43974872/197236367-38c75873-8ab6-4b18-a223-5c70fd43c5c2.png)

### Ratings DataFrame

![image](https://user-images.githubusercontent.com/43974872/197238093-85759d41-77ff-40c5-9794-47c45edfdfdd.png)



# 2. Extract and Transform the Wikipedia Data
Extract and transform the Wikipedia data so we can merge it with the Kaggle metadata. While extracting the IMDb IDs using a regular expression string and dropping duplicates, use a try-except block to catch errors.
```
# Add the function that takes in three arguments; Wikipedia data, Kaggle metadata, and MovieLens rating data (from Kaggle)

def etl():
    #Read in the kaggle metadata and MovieLens ratings CSV files as Pandas DataFrames.
    kaggle_metadata = pd.read_csv('movies_metadata.csv', low_memory=False)
    ratings = pd.read_csv('ratings.csv')
    
    #Open the read the Wikipedia data JSON file.
    with open('wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
        

    # 3. Write a list comprehension to filter out TV shows.
        wiki_movies = [movie for movie in wiki_movies_raw
                   if ('Director' in movie or 'Directed by' in movie)
                       and 'imdb_link' in movie
                       and 'No. of episodes' not in movie]

    # 4. Write a list comprehension to iterate through the cleaned wiki movies list
    # and call the clean_movie function on each movie.
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    # 5. Read in the cleaned movies list from Step 4 as a DataFrame.
    wiki_movies_df = pd.DataFrame(clean_movies)


    # 6. Write a try-except block to catch errors while extracting the IMDb ID using a regular expression string and
    #  dropping any imdb_id duplicates. If there is an error, capture and print the exception.
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        
    except Exception as e:
        print(e)

    #  7. Write a list comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() <= len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # 8. Create a variable that will hold the non-null values from the “Box office” column.
    box_office = wiki_movies_df['Box office'].dropna()
    
    # 9. Convert the box office data created in Step 8 to string values using the lambda and join functions.
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
 
    # 10. Write a regular expression to match the six elements of "form_one" of the box office data.
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    
    # 11. Write a regular expression to match the three elements of "form_two" of the box office data.
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)  
    
    # 12. Add the parse_dollars function.
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan
        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a million
            value = float(s) * 10**6
            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            # convert to float and multiply by a billion
            value = float(s) * 10**9
            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove dollar sign and commas
            s = re.sub('\$|,','', s)
            # convert to float
            value = float(s)
            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan
            
    # 13. Clean the box office column in the wiki_movies_df DataFrame.
    # Extract the values
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    #Drop
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # 14. Clean the budget column in the wiki_movies_df DataFrame.
    # Create a budget variable
    budget = wiki_movies_df['Budget'].dropna()
    
    #Convert any lists to strings:
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # Remove the citation references
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    
    # recycling form_one and form_two from box_office work
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # 15. Clean the release date column in the wiki_movies_df DataFrame.
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # month, dd, yyyy
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    
    # yyyy-mm-dd and yyyy/mm/dd
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    
    # month yyyy
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    
    #yyyy
    date_form_four = r'\d{4}'
    
    ## Instead of creating our own function to parse the dates, we'll use the built-in to_datetime() method in Pandas.
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)

    # 16. Clean the running time column in the wiki_movies_df DataFrame.
    # Create a variable
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Extract values but only digits
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    
    # convert them into numeric value
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    
    # Apply a function 
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    #Drop
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    # Return three variables. The first is the wiki_movies_df DataFrame
    
    return wiki_movies_df, kaggle_metadata, ratings
```
### Wiki_Movies Clean DataFrame

![image](https://user-images.githubusercontent.com/43974872/197296591-2ed90218-ebc2-43a7-9a0e-db506dec49e0.png)
![image](https://user-images.githubusercontent.com/43974872/197297579-cc939ed4-7497-4517-aa18-5f33dec1b82e.png)

### List of columns

![image](https://user-images.githubusercontent.com/43974872/197296686-dc596495-2430-4ac8-97f0-c5ee1ef97916.png)


# 3. Extract and Transform the Kaggle Data
Extract and transform the Kaggle metadata and MovieLens rating data, then convert the transformed data into separate DataFrames. Then, you’ll merge the Kaggle metadata DataFrame with the Wikipedia movies DataFrame to create the movies_df DataFrame. Finally, you’ll merge the MovieLens rating data DataFrame with the movies_df DataFrame to create the movies_with_ratings_df.

# 4. Create the Movie Database
Add the movies_df DataFrame and MovieLens rating CSV data to a SQL database.

# Conecting Movies_data and ratings to SQL


## Movies

![image](https://user-images.githubusercontent.com/43974872/197107143-e00c87aa-92b0-4ea2-9b06-4efa95e9bbe2.png)

## Ratings

![image](https://user-images.githubusercontent.com/43974872/197106982-12b55286-fc7a-4e57-8f8f-3a019e56623b.png)

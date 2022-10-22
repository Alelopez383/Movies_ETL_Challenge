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
Extract and transform the Kaggle metadata and MovieLens rating data, then convert the transformed data into separate DataFrames. Then, merge the Kaggle metadata DataFrame with the Wikipedia movies DataFrame to create the movies_df DataFrame. Finally,merge the MovieLens rating data DataFrame with the movies_df DataFrame to create the movies_with_ratings_df.

The function used in this part of the analysis, use the two functions we already create in Delivery 1 and Delivery 2, so I´m quoting the part that is new.
On and matter, When dataframes were created, I also checked the values of each column of each dataframe to keep it for the next step, importing the datasets to SQL. 
```

###############################################################################
#------------------Function from Deliverable 1--------------------------------#
#------------------Function from Deliverable 2--------------------------------#
###############################################################################

     # 2. Clean the Kaggle metadata.
    kaggle_metadata.dtypes
    kaggle_metadata['adult'].value_counts()
    kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    kaggle_metadata['video'].value_counts()
    kaggle_metadata['video'] == 'True'
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    # 3. Merged the two DataFrames into the movies DataFrame.
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    
    # 4. Drop unnecessary columns from the merged DataFrame.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    # 5. Add in the function to fill in the missing Kaggle data.
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
             , axis=1)
        df.drop(columns=wiki_column, inplace=True)
        
    # 6. Call the function in Step 5 with the DataFrame and columns as the arguments.
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # 7. Filter the movies DataFrame for specific columns.
    # one way to reorder the columns
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                           'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                           'genres','original_language','overview','spoken_languages','Country',
                           'production_companies','production_countries','Distributor',
                           'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                          ]]

    # 8. Rename the columns in the movies DataFrame.
    movies_df.rename({'id':'kaggle_id',
                      'title_kaggle':'title',
                      'url':'wikipedia_url',
                      'budget_kaggle':'budget',
                      'release_date_kaggle':'release_date',
                      'Country':'country',
                      'Distributor':'distributor',
                      'Producer(s)':'producers',
                      'Director':'director',
                      'Starring':'starring',
                      'Cinematography':'cinematography',
                      'Editor(s)':'editors',
                      'Writer(s)':'writers',
                      'Composer(s)':'composers',
                      'Based on':'based_on'
                     }, axis='columns', inplace=True)
    
    # 9. Transform and merge the ratings DataFrame.
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1)
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                    .rename({'userId':'count'}, axis=1) \
                    .pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)

    return wiki_movies_df, movies_with_ratings_df, movies_df
```
### Wiki_Movies DataFrame

![image](https://user-images.githubusercontent.com/43974872/197305855-66760f52-9c9f-4e6b-939f-87ee320f9d6e.png)

![image](https://user-images.githubusercontent.com/43974872/197306024-a5405c24-120e-4b23-9535-d7dfc9d7c526.png)

### Movies_with_ratings DataFrame

![image](https://user-images.githubusercontent.com/43974872/197306075-f5f75fc3-1e10-422e-97da-bd5af06174cf.png)

![image](https://user-images.githubusercontent.com/43974872/197306124-3d430ca8-662e-432a-ad74-fc9a6f3de607.png)

### Movies DataFrame

![image](https://user-images.githubusercontent.com/43974872/197306151-a9b9dcc8-fc0c-470c-b625-0b63cd2b3316.png)

![image](https://user-images.githubusercontent.com/43974872/197306167-ed4eb867-82d2-4aa4-bdbe-f56aac8fa7eb.png)


# 4. Create the Movie Database
Add the movies_df DataFrame and MovieLens rating CSV data to a SQL database.

```
###############################################################################
#------------------Function from Deliverable 1--------------------------------#
#------------------Function from Deliverable 2--------------------------------#
#------------------Function from Deliverable 3--------------------------------#
###############################################################################
 #10. Add the code to create the connection to the PostgreSQL
    db_string = f"postgresql://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')
```
Because Ratings is a big database to import in one statement, it has to be divided into "chunks" of data. 

### Importing rows

![image](https://user-images.githubusercontent.com/43974872/197311340-18fb14e9-73ac-4927-9837-cd1effcbc106.png)


# Conecting Movies_data and ratings to SQL


## Movies

![image](https://user-images.githubusercontent.com/43974872/197107143-e00c87aa-92b0-4ea2-9b06-4efa95e9bbe2.png)

## Ratings

![image](https://user-images.githubusercontent.com/43974872/197106982-12b55286-fc7a-4e57-8f8f-3a019e56623b.png)

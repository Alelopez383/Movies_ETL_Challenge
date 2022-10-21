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
 ```   
### Wiki_movies DataFrame

![image](https://user-images.githubusercontent.com/43974872/197236109-9cbf3fdd-10c7-4749-a1c0-5a12170e1ed7.png)

### Kaggle_metadata DataFrame

![image](https://user-images.githubusercontent.com/43974872/197236367-38c75873-8ab6-4b18-a223-5c70fd43c5c2.png)

### Ratings DataFrame

![image](https://user-images.githubusercontent.com/43974872/197238093-85759d41-77ff-40c5-9794-47c45edfdfdd.png)



# 2. Extract and Transform the Wikipedia Data
Extract and transform the Wikipedia data so you can merge it with the Kaggle metadata. While extracting the IMDb IDs using a regular expression string and dropping duplicates, use a try-except block to catch errors.

# 3. Extract and Transform the Kaggle Data
Extract and transform the Kaggle metadata and MovieLens rating data, then convert the transformed data into separate DataFrames. Then, you’ll merge the Kaggle metadata DataFrame with the Wikipedia movies DataFrame to create the movies_df DataFrame. Finally, you’ll merge the MovieLens rating data DataFrame with the movies_df DataFrame to create the movies_with_ratings_df.

# 4. Create the Movie Database
Add the movies_df DataFrame and MovieLens rating CSV data to a SQL database.

# Conecting Movies_data and ratings to SQL


## Movies

![image](https://user-images.githubusercontent.com/43974872/197107143-e00c87aa-92b0-4ea2-9b06-4efa95e9bbe2.png)

## Ratings

![image](https://user-images.githubusercontent.com/43974872/197106982-12b55286-fc7a-4e57-8f8f-3a019e56623b.png)

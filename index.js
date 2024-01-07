const { Client } = require("pg");

const client = new Client({
  user: "database_squadcast_user",
  host: "dpg-cm9v27a1hbls73ak93mg-a.oregon-postgres.render.com",
  database: "database_squadcast",
  password: "eZDRW57lrRcHG2w73caRfYfE5Lka1xQV",
  port: 5432,
  ssl: {
    rejectUnauthorized: false,
  },
  connectionTimeoutMillis: 5000, // Adjust timeout
  query_timeout: 5000, //
});

// Define an asynchronous function to get the top movies by duration
async function getTopMoviesByDuration() {
  try {
    await client.connect(); // Connect to the PostgreSQL database using the client

    //a) top moviest by duration
    const query = `
      SELECT title, minutes
      FROM movies
      ORDER BY minutes DESC
      LIMIT 5;
    `;

    const result = await client.query(query);
    const topMovies = result.rows;

    console.log("\nTop 5 Movies based on Duration:\n");
    topMovies.forEach((movie) => console.log(movie.title, movie.minutes));

    // b)  Top 5 movies based on year of release :
    const query2 = "SELECT title, year FROM movies ORDER BY year DESC LIMIT 5;";
    const result2 = await client.query(query2);
    const topMovies2 = result2.rows;
    console.log("\nTop 5 Movies based on Year of Release:\n");
    topMovies2.forEach((movie) => console.log(movie.title, movie.year));

    // c) Top 5 movies based on number of ratings given :
    const query3 = `
      SELECT m.id, m.title, COUNT(r.rating) AS num_ratings
      FROM movies m
      LEFT JOIN ratings r ON m.id = r.movie_id
      GROUP BY m.id, m.title
      ORDER BY num_ratings DESC
      LIMIT 5;
    `;
    const result3 = await client.query(query3);
    const topMovies3 = result3.rows;
    console.log("\nTop 5 Movies based on Average Ratings:\n");
    topMovies3.forEach((movie) => {
      console.log(`${movie.title} - ${movie.num_ratings} ratings`);
    });

    // d) Top 5 movies based on number of Average Ratings ::
    const query4 = `SELECT m.title, AVG(r.rating) AS avg_rating
                    FROM movies m
                    JOIN ratings r ON m.id = r.movie_id
                    GROUP BY m.title
                    HAVING COUNT(r.rating) >= 5
                    ORDER BY avg_rating DESC
                    LIMIT 5;`;
    const result4 = await client.query(query4);
    const topMovies4 = result4.rows;
    console.log("\nTop 5 Movies based on Average Ratings (>= 5 ratings):\n");
    topMovies4.forEach((movie) => {
      console.log(`${movie.title} - ${movie.avg_rating} average rating`);
    });

    //Number of Unique Raters:
    const query5 = `
      SELECT COUNT(DISTINCT rater_id) AS num_unique_raters
      FROM ratings;
    `;

    // Execute the SQL query using the client and get the result
    const result5 = await client.query(query5);
    // Extract the count of unique rater IDs from the result
    const numUniqueRaters = result5.rows[0].num_unique_raters;

    // Print the count of unique rater IDs
    console.log("\nNumber of Unique Raters:", numUniqueRaters);

    //Top 5 Rater IDs: Sort and print the top 5 rater IDs based on:
    // ● Most movies rated
    const query6 = `
      SELECT rater_id, COUNT(DISTINCT movie_id) AS num_movies_rated
      FROM ratings
      GROUP BY rater_id
      ORDER BY num_movies_rated DESC
      LIMIT 5;
    `;
    const result6 = await client.query(query6);
    const topRaterIDs = result6.rows;

    // Print a message and display the top 5 rater IDs
    console.log("\nTop 5 Rater IDs based on Most Movies Rated:\n");
    topRaterIDs.forEach((rater) => {
      console.log(
        `Rater ID: ${rater.rater_id} - Movies Rated: ${rater.num_movies_rated}`,
      );
    });

    //Top 5 Rater IDs: Sort and print the top 5 rater IDs based on:
    // ● Highest average rating
    const query7 = `SELECT rater_id, AVG(rating) AS avg_rating
                    FROM ratings
                    GROUP BY rater_id
                    HAVING COUNT(rating) >= 5
                    ORDER BY avg_rating DESC
                    LIMIT 1;
                    `;
    const result7 = await client.query(query7);
    const topRaterIDs2 = result7.rows[0];
    // Print a message and display the rater with the highest average rating
    console.log(
      "\nRater with the Highest Average Rating (considering raters with min 5 ratings):\n",
    );
    console.log(
      `Rater ID: ${topRaterIDs2.rater_id} - Average Rating: ${topRaterIDs2.avg_rating}`,
    );

    //

    //Top Rated Movie:
    //- Find and print the top-rated movies by:
    //● Director 'Michael Bay'

    const query8 = `
      SELECT m.title, AVG(r.rating) AS avg_rating
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      WHERE m.director = 'Michael Bay'
      GROUP BY m.title
      HAVING COUNT(r.rating) >= 5
      ORDER BY avg_rating DESC
      LIMIT 5;
    `;
    const result8 = await client.query(query);

    // Extract the rows from the result
    const topRatedMovies = result8.rows;

    // Print a message and display the top-rated movies
    console.log('\nTop Rated Movies by Director "Michael Bay":\n');
    topRatedMovies.forEach((movie) => {
      console.log(
        `Movie Title: ${movie.title} - Average Rating: ${movie.avg_rating}`,
      );
    });

    //- Find and print the top-rated movies by: ● 'Comedy',
    const query9 = `
      SELECT m.title, AVG(r.rating) AS avg_rating
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      WHERE m.genre LIKE '%Comedy%'
      GROUP BY m.title
      HAVING COUNT(r.rating) >= 5
      ORDER BY avg_rating DESC
      LIMIT 5;
    `;

    const result9 = await client.query(query9);
    // Extract the rows from the result
    const topRatedComedyMovies = result9.rows;
    // Print a message and display the top-rated movies
    console.log('\nTop Rated Movies by Genre "Comedy":\n');
    topRatedComedyMovies.forEach((movie) => {
      console.log(
        `Movie Title: ${movie.title} - Average Rating: ${movie.avg_rating}`,
      );
    });

    //● In the year 2013
    const query10 = `
        SELECT m.title, AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON m.id = r.movie_id
        WHERE m.year::integer = 2013
        GROUP BY m.title
        HAVING COUNT(r.rating) >= 5
        ORDER BY avg_rating DESC
        LIMIT 5;
      `;
    const result10 = await client.query(query10);
    // Extract the rows from the result
    const topRatedMoviesByYear = result10.rows;

    // Print a message and display the top-rated movies in the year 2013
    console.log("\nTop Rated Movies in the Year 2013:\n");
    topRatedMoviesByYear.forEach((movie) => {
      console.log(
        `Movie Title: ${movie.title} - Average Rating: ${movie.avg_rating}`,
      );
    });

    //Find and print the top-rated movies by: ● In India
    const query11 = `
        SELECT m.title, AVG(r.rating) AS avg_rating
        FROM movies m
        JOIN ratings r ON m.id = r.movie_id
        WHERE m.country ILIKE '%India%'
        GROUP BY m.title
        HAVING COUNT(r.rating) >= 5
        ORDER BY avg_rating DESC
        LIMIT 5;
      `;

    const result11 = await client.query(query11);
    // Extract the rows from the result

    const topRatedMoviesByCountry = result11.rows;
    console.log("\nTop Rated Movies in India:\n");
    // Print
    topRatedMoviesByCountry.forEach((movie) => {
      console.log(
        `Movie Title: ${movie.title} - Average Rating: ${movie.avg_rating}`,
      );
    });
    /*Favorite Movie Genre of Rater ID 1040: Determine and print the favorite movie genre
    for the rater with ID 1040 (defined as the genre of the movie the rater has rated most often). */
    const query12 = `SELECT m.genre, COUNT(*) AS genre_count
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      WHERE r.rater_id = 1040
      GROUP BY m.genre
      ORDER BY genre_count DESC
      LIMIT 1;`;

    const result12 = await client.query(query12);
    const favoriteGenre = result12.rows[0];

    console.log("\nFavorite Movie Genre for Rater ID 1040:\n");
    console.log(
      `Genre: ${favoriteGenre.genre} - Count: ${favoriteGenre.genre_count}`,
    );

    /*Highest Average Rating for a Movie Genre by Rater ID 1040: Find and print the
    highest average rating for a movie genre given by the rater with ID 1040 (consider genres with a
    minimum of 5 ratings).*/

    // Define the SQL query to get the highest average rating for a movie genre by rater ID 1040
    const query13 = `
      SELECT m.genre, AVG(r.rating) AS avg_rating
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      WHERE r.rater_id = 1040
      GROUP BY m.genre
      HAVING COUNT(r.rating) >= 5
      ORDER BY avg_rating DESC
      LIMIT 1;
    `;
    // Execute the query and get the result
    const result13 = await client.query(query13);
    const highestAverageRatingInfo = result13.rows[0];
    // Print a message and display the highest average rating for a movie genre by rater ID 1040
    console.log(
      "\nHighest Average Rating for a Movie Genre by Rater ID 1040:\n",
    );
    console.log(
      `Genre: ${highestAverageRatingInfo.genre} - Average Rating: ${highestAverageRatingInfo.avg_rating}`,
    );

    //Year with Second-Highest Number of Action Movies:
    const query14 = `
      SELECT m.year, COUNT(*) AS action_movie_count
      FROM movies m
      WHERE m.country ILIKE '%USA%'
        AND m.genre ILIKE '%Action%'
        AND m.minutes < 120
      GROUP BY m.year
      HAVING AVG((SELECT AVG(rating) FROM ratings r WHERE r.movie_id = m.id)) >= 6.5
      ORDER BY action_movie_count DESC
      OFFSET 1
      LIMIT 1;
    `;
    const result14 = await client.query(query14);
    const yearWithSecondHighestActionMovies = result14.rows[0];
    console.log("\nYear with Second-Highest Number of Action Movies:\n");
    console.log(
      `Year: ${yearWithSecondHighestActionMovies.year} - Action Movie Count: ${yearWithSecondHighestActionMovies.action_movie_count}`,
    );

    /* h) Count of Movies with High Ratings: Count and print the number of movies that       have received at least five reviews with a rating of 7 or higher*/

    const query15 = `
                SELECT m.title, COUNT(r.movie_id) AS review_count
                FROM movies m
                JOIN ratings r ON m.id = r.movie_id
                WHERE r.rating >= 7
                GROUP BY m.id, m.title
                HAVING COUNT(r.movie_id) >= 5;
    `;
    const result15 = await client.query(query15);
    // Extract the count of movies with high ratings from the result
    //const countOfHighRatedMovies = result15.rows[0];

    // Print a message and display the count of movies with high ratings
    console.log(
      "\nMovies with at least five reviews and a rating of 7 or higher:\n",
    );
    result15.rows.forEach((row) =>
      console.log(`${row.title} - Reviews: ${row.review_count}`),
    );

    //catch ---
  } catch (error) {
    // Handle errors if the query execution fails
    console.error("Error executing query:", error.message);
  } finally {
    // Ensure to close the database connection regardless of success or failure
    await client.end();
  }
}
getTopMoviesByDuration();
// For question   go to numberofunique.js

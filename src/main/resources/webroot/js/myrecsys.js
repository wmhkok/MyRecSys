function addRowFrame(pageId, rowName, rowId, baseUrl) {
    var html = `
        <div class="row-frame">
            <a class="plainlink" title="go to the full list" href="${baseUrl}collection.html?type=genre&value=${rowName}">${rowName}</a>
            <div class="movie-row" id="${rowId}"></div>
        </div>`;
    document.getElementById(pageId).insertAdjacentHTML('afterbegin', html)
};

function appendMovie2Row(rowId, movieName, movieId, year, rating, rateNumber, genres, baseUrl) {
    var genresStr = genres.map(function(genre) {
        return '<div class="genre"><a href="' + baseUrl + 'collection.html?type=genre&value=' + genre + '"><b>' + genre + '</b></a></div>';
    }).join('');
    var html = `
        <div class="movie-row-item">
            <div class="movie-card">
                <a href="./movie.html?movieId=${movieId}">
                    <div class="poster">
                        <img src="./posters/${movieId}.jpg"/>
                    </div>
                </a>
                <div class="overlay">
                    <a href="./movie.html?movieId=${movieId}">
                        <p class="title">${movieName}</p>
                    </a>
                    <div class="rating-indicator">
                        <svg xmlns="http://www.w3.org/2000/svg" class="star-icon" height="14px" viewBox="0 0 14 14" width="14px">
                            <polygon fill-rule="evenodd" points="13.7714286 5.4939887 9.22142857 4.89188383 7.27142857 0.790044361 5.32142857 4.89188383 0.771428571 5.4939887 4.11428571 8.56096041 3.25071429 13.0202996 7.27142857 10.8282616 11.2921429 13.0202996 10.4285714 8.56096041" stroke="none"></polygon>
                        </svg>
                        <p class="rating-value">${rating}</p>
                    </div>
                    <p class="year">${year}</p>
                    <p class="genre-list">${genresStr}</p>
                    <div class="rating-detail">
                        <span style="font-size: 18px; font-weight: bold;">${rating}</span><span>/5 in</span>
                        <div class="total-rating-number">${rateNumber} ratings</div>
                    </div>
                </div>
            </div>
        </div>`;
    document.getElementById(rowId).insertAdjacentHTML('beforeend', html)
}

function addGenreRow(pageId, rowName, rowId, size, baseUrl) {
    addRowFrame(pageId, rowName, rowId, baseUrl);

    fetch(baseUrl + "getrecommendation?genre=" + rowName + "&size=" + size + "&sortby=rating")
        .then(response => response.json())
        .then(result => {
            result.forEach(movie => {
                appendMovie2Row(rowId, movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres, baseUrl)
            });
        })
};
function addUserDetails(blockId, userId, baseUrl) {
    fetch(baseUrl + "getuser?id=" + userId)
    .then(response => response.json())
    .then(userObject => {
        var html = `
            <div class="detail-container">
                <div class="poster">
                    <img src="./usericon/${userId % 5}.png"/>
                </div>
                <div class="content">
                    <h1 class="title">User ${userObject.userId}</h1>
                    <div class="rating-count">
                        <div>watched Movie Numbers</div>
                        <div style="font-weight: bold;">${userObject.ratingCount}</div>
                    </div>
                    <div class="user-average-rating">
                        <div>Average Rating Score</div>
                        <div style="font-weight: bold;">
                            ${userObject.averageRating.toPrecision(2)}
                            <svg xmlns="http://www.w3.org/2000/svg" class="star-icon" height="14px" viewBox="0 0 14 14" width="14px">
                                <polygon fill-rule="evenodd" points="13.7714286 5.4939887 9.22142857 4.89188383 7.27142857 0.790044361 5.32142857 4.89188383 0.771428571 5.4939887 4.11428571 8.56096041 3.25071429 13.0202996 7.27142857 10.8282616 11.2921429 13.0202996 10.4285714 8.56096041" stroke="none"></polygon>
                            </svg>
                        </div>
                    </div>
                    <div class="highest-rating">
                        <div>Highest Rating Score</div>
                        <div style="font-weight: bold;">
                            ${userObject.highestRating.toPrecision(2)}
                            <svg xmlns="http://www.w3.org/2000/svg" class="star-icon" height="14px" viewBox="0 0 14 14" width="14px">
                                <polygon fill-rule="evenodd" points="13.7714286 5.4939887 9.22142857 4.89188383 7.27142857 0.790044361 5.32142857 4.89188383 0.771428571 5.4939887 4.11428571 8.56096041 3.25071429 13.0202996 7.27142857 10.8282616 11.2921429 13.0202996 10.4285714 8.56096041" stroke="none"></polygon>
                            </svg>
                        </div>
                    </div>
                    <div class="lowest-rating">
                        <div>Lowest Rating Score</div>
                        <div style="font-weight: bold;">
                            ${userObject.lowestRating.toPrecision(2)}
                            <svg xmlns="http://www.w3.org/2000/svg" class="star-icon" height="14px" viewBox="0 0 14 14" width="14px">
                                <polygon fill-rule="evenodd" points="13.7714286 5.4939887 9.22142857 4.89188383 7.27142857 0.790044361 5.32142857 4.89188383 0.771428571 5.4939887 4.11428571 8.56096041 3.25071429 13.0202996 7.27142857 10.8282616 11.2921429 13.0202996 10.4285714 8.56096041" stroke="none"></polygon>
                            </svg>
                        </div>
                    </div>
                </div>
            </div>`;
        document.getElementById(blockId).insertAdjacentHTML('afterbegin', html)
    })
};
function addMovieDetails(blockId, movieId, baseUrl) {
    fetch(baseUrl + "getmovie?id=" + movieId)
        .then(response => response.json())
        .then(movieObject => {
            let genres = "";
            movieObject.genres.forEach((genre, i) => {
                genres += `<span><a href="${baseUrl}collection.html?type=genre&value=${genre}"><b>${genre}</b></a>`;
                if (i < movieObject.genres.length - 1) {
                    genres += ", </span>";
                } else {
                    genres += "</span>";
                }
            });

            let ratingUsers = "";
            movieObject.topRatings.forEach((rating, i) => {
                ratingUsers += `<span><a href="${baseUrl}user.html?id=${rating.rating.userId}"><b>User${rating.rating.userId}</b></a>`;
                if (i < movieObject.topRatings.length - 1) {
                    ratingUsers += ", </span>";
                } else {
                    ratingUsers += "</span>";
                }
            });

            var html = `
                <div class="detail-container">
                    <div class="poster" >
                        <img src="./posters/${movieId}.jpg"/>
                    </div>
                    <div class="content">
                        <h1 class="title">${movieObject.title}</h1>
                        <div class="year">
                            <div>ReleaseYear</div>
                            <div style="font-weight: bold;">${movieObject.releaseYear}</div>
                        </div>
                        <div class="average-rating">
                            <div>Average of ${movieObject.ratingNumber} ratings</div>
                            <div style="font-weight: bold;">
                                ${movieObject.averageRating.toPrecision(2)} 
                                <svg xmlns="http://www.w3.org/2000/svg" class="star-icon" height="14px" viewBox="0 0 14 14" width="14px">
                                    <polygon fill-rule="evenodd" points="13.7714286 5.4939887 9.22142857 4.89188383 7.27142857 0.790044361 5.32142857 4.89188383 0.771428571 5.4939887 4.11428571 8.56096041 3.25071429 13.0202996 7.27142857 10.8282616 11.2921429 13.0202996 10.4285714 8.56096041" stroke="none"></polygon>
                                </svg>
                            </div>
                        </div>
                        <div class="links">
                            <div>Links</div>
                            <div style="font-weight: bold;">
                                <span><a target="_blank" href="http://www.imdb.com/title/tt${movieObject.imdbId}">imdb</a>,</span>
                                <span><a target="_blank" href="http://www.themoviedb.org/movie/${movieObject.tmdbId}">tmdb</a></span>
                            </div>
                        </div>
                        <div class="genres">
                            <div>Genres</div>
                            <div>${genres}</div>
                        </div>
                        <div class="high-rating-users">
                            <div>who likes the movie most</div>
                            <div>${ratingUsers}</div>
                        </div>
                    </div>
                </div>`;
            document.getElementById(blockId).insertAdjacentHTML('afterbegin', html)
        })
};
function addUserHistory(blockId, userId, baseUrl) {
    var html =`
        <div class="row-frame">
            <div>Uesr Watched</div>
            <div class="movie-row" id="user-history"></div>
        </div>`;
    document.getElementById(blockId).insertAdjacentHTML('beforeend', html)

    fetch(baseUrl + "getuser?id=" + userId)
        .then(response => response.json())
        .then(userObject => {
            userObject.ratings.forEach(rating => {
                fetch(baseUrl + "getmovie?id=" + rating.rating.movieId)
                    .then(response => response.json())
                    .then(movie => {
                        appendMovie2Row("user-history", movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres, baseUrl)
                    })
            })
        })
};

function addRecForYou(blockId, userId, baseUrl) {
    var html =`
        <div class="row-frame">
            <div>Recommended For This User</div>
            <div class="movie-row" id="rec-for-you"></div>
        </div>`;
    document.getElementById(blockId).insertAdjacentHTML('beforeend', html)

    fetch(baseUrl + "getrecforyou?id=" + userId + "&size=18")
        .then(response => response.json())
        .then(movies => {
            console.log(movies)
            movies.forEach(movie => {
                appendMovie2Row("rec-for-you", movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres, baseUrl)
            })
        })
};

function addRelatedMovies(blockId, movieId, baseUrl) {
    var html =`
        <div class="row-frame">
            <div>Related Movies</div>
            <div class="movie-row" id="related-movies"></div>
        </div>`;
    document.getElementById(blockId).insertAdjacentHTML('beforeend', html)

    fetch(baseUrl + "getsimilarmovie?movieId=" + movieId + "&size=20")
        .then(response => response.json())
        .then(result => {
            result.forEach(movie => {
                appendMovie2Row("related-movies", movie.title, movie.movieId, movie.releaseYear, movie.averageRating.toPrecision(2), movie.ratingNumber, movie.genres, baseUrl)
            })
        })
};
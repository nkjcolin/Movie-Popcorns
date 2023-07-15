from django.urls import path

from . import views


urlpatterns = [
    path('login/', views.login_view, name='login_view'),                                            # User presses to view login page
    path("logout/", views.logout_view, name='logout_view'),                                         # User presses to logout 
    path('register/', views.register, name='register'),                                             # User presses to view registration page

    path('homepage/', views.homepage, name='homepage'),                                             # User presses to view homepage
    path('recommend/', views.recommend_movies, name='recommend'),                                   # Display movie recommendations
    path(r'movie/search/<str:title>', views.movieSearch, name='movieSearch'),                       # User searches for a movie         
    path(r'movie/<int:titleID>', views.movie, name='movie'),                                        # User presses to view movie page       

    path('genre/', views.genre, name='genre'),                                                      # User presses to choose genre
    path('genre/genreSelect/<str:genreselection>/', views.genreSelect, name='genreSelect'),         # Display genre selected

    path('cast/', views.cast, name='cast'),                                                         # Display the cast list
    path('cast/castSelect/<slug:cast>/', views.castSelect, name='castSelect'),                      # Display cast selected
    path(r'cast/search/<str:cast>', views.castSelect, name='castSelect'),                           # User searches for a cast         

    path('profile/', views.profile, name='profile'),                                                # User presses to view profile page
    path('account/', views.account, name='account'),                                                # User presses to view account page

    path('cast/movies/list/<int:cast_id>/', views.movie_list_by_cast, name='movie_list_by_cast')
]

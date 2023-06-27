from django.urls import path

from . import views


urlpatterns = [
    path('homepage/', views.homepage, name='homepage'),             # User presses to view homepage
    path(r'movie/<int:titleID>', views.movie, name='movie'),        # User presses to view movie page       
    path(r'movie/<str:title>', views.movieSearch, name='movieSearch'),    # User searches for a movie         

    path('movie/', views.account, name='movie'),                    # User presses to view movie page
    path('actor/', views.account, name='actor'),                    # User presses to view actor page
    path('account/', views.account, name='account'),                # User presses to view account page

    path('profile/', views.profile, name='profile'),                # User presses to view profile page
    path("logout/",views.logout_view,name="logout"),                # user logout 
    path('login/', views.login_view, name='login'),                 # User presses to view login page
    path('register/', views.signup, name='register'),               # User presses to view registration page
    
    path('sorted-movies/', views.sorted_movies, name='sorted_movies'),
]

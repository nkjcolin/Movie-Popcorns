from django.urls import path

from . import views


urlpatterns = [
    path('dashboard/', views.index, name='index'),          # User presses to view homepage

    path('login/', views.login, name='login'),              # User presses to view login page
    path('register/', views.register, name='register'),     # User presses to view registration page
    
    path('movie/', views.account, name='movie'),          # User presses to view movie page
    path('actor/', views.account, name='actor'),          # User presses to view actor page
    path('profile/', views.profile, name='profile'),        # User presses to view profile page
    path('account/', views.account, name='account'),        # User presses to view account page
]

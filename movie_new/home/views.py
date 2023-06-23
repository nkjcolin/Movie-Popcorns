from django.shortcuts import render,HttpResponseRedirect
from django.contrib.auth import authenticate,login,logout
from django.contrib.auth.models import User,Group
from django.contrib.auth.forms import UserCreationForm,AuthenticationForm
from django.contrib import messages
from .models import titleInfo,userAccount



def index(request):
    segment = "dashboard"
    context = {'segment': segment}

    return render(request, 'pages/dashboard.html', context)


def movie(request):
    segment = "movie"
    context = {'segment': segment}

    return render(request, 'pages/movie.html', context)

def actor(request):
    segment = "actor"
    context = {'segment': segment}

    return render(request, 'pages/actor.html', context)

def account(request):
    segment = "account"
    context = {'segment': segment}

    return render(request, 'pages/account.html', context)


#shows the user total watched movie and rating so far 
def profile(request):
    if request.user.is_authenticated:
        # filter UserAccount objects based on the userID field
        r = userAccount.objects.filter(userID=request.user.id)
        totalReview = 0
        for item in r:
            totalReview += int(item.rating)
        # filter the objects based on the userID field, where the userID matches the request.user.id
        totalWatchedMovie = userAccount.objects.filter(userID=request.user.id).count()
        return render(request, 'pages/profile.html', {'totalWatchedMovie': totalWatchedMovie, 'totalReview': totalReview})
    else:
        return HttpResponseRedirect('/login/')
    
def logout_view(request):
    if request.user.is_authenticated:
        logout(request)
        return HttpResponseRedirect('/login/')
    
def login_view(request):
    if not request.user.is_authenticated:
        if request.method == 'POST':
            fm = AuthenticationForm(request=request, data=request.POST)
            if fm.is_valid():
                uname = fm.cleaned_data['username']
                upass = fm.cleaned_data['password']
                user = authenticate(username=uname, password=upass)
                if user is not None:
                    login(request, user)
                    messages.success(request, 'Logged in Successfully!!')
                    return HttpResponseRedirect('/dashboard/')
        else:
            fm = AuthenticationForm()
        return render(request, 'pages/login.html', {'form': fm})
    else:
        return HttpResponseRedirect('/dashboard/')
    
def signup(request):
    if not request.user.is_authenticated:
        if request.method == 'POST':
            fm = UserCreationForm(request.POST)
            if fm.is_valid():
                user = fm.save()
                group = Group.objects.get(name='Editor')
                user.groups.add(group)
                messages.success(request, 'Account Created Successfully!!!')
                return HttpResponseRedirect('/login/')
        else:
            fm = UserCreationForm()
        return render(request, 'pages/register.html', {'form': fm})
    else:
        return HttpResponseRedirect('/home/')


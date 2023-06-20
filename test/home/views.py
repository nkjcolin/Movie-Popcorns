from django.shortcuts import render


def index(request):
    segment = "dashboard"
    context = {'segment': segment}

    return render(request, 'pages/dashboard.html', context)

def login(request):
    return render(request, 'pages/login.html')

def register(request):
    return render(request, 'pages/register.html')

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


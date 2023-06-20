from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.shortcuts import render,HttpResponseRedirect
from django.contrib.auth.models import User,Group
from .forms import LoginForm,SignUpForm

def home(request):
    return render(request, 'home.html', {})


def signup(request):
    if not request.user.is_authenticated:
        if request.method=='POST':
            fm=SignUpForm(request.POST)
            if fm.is_valid():
                user=fm.save()
                group=Group.objects.get(name='Editor')
                user.groups.add(group)
                messages.success(request,'Account Created Successfully!!!')
        else:
            if not request.user.is_authenticated:
                fm=SignUpForm()
        return render(request,'signup.html',{'form':fm})
    else:
        return HttpResponseRedirect('/home/')

def user_login(request):
    if not request.user.is_authenticated:
        if request.method=='POST':
            fm=LoginForm(request=request,data=request.POST)
            if fm.is_valid():
                uname=fm.cleaned_data['username']
                upass=fm.cleaned_data['password']
                user=authenticate(username=uname,password=upass)
                if user is not None:
                    login(request,user)
                    messages.success(request,'Logged in Successfully!!')
                    return HttpResponseRedirect('/dashboard/')
        else:
            fm=LoginForm()
        return render(request,'login.html',{'form':fm})
    else:
        return HttpResponseRedirect('/dashboard/')
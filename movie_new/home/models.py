from django.db import models

class titleInfo(models.Model):
    titleID = models.IntegerField(primary_key=True)
    title = models.CharField(max_length=100)
    genre = models.CharField(max_length=20)
    runtime = models.IntegerField()
    yearReleased = models.DateField()
    
    def __str__(self):
        return str(self.titleID)

class userAccount(models.Model):
    userID = models.IntegerField(primary_key=True)
    username = models.CharField(max_length=30)
    password = models.CharField(max_length=30)
    rating = models.IntegerField()
    movie = models.CharField(max_length=30)

    def __str__(self):
        return self.username

class UserMap(models.Model):
    mappingID = models.AutoField(primary_key=True)
    titleID = models.ForeignKey(titleInfo, on_delete=models.CASCADE)
    userID = models.ForeignKey(userAccount, on_delete=models.CASCADE)
    
    def __str__(self):
        return f"UserMap ID: {self.mappingID}"

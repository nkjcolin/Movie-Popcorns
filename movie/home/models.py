from django.db import models


class userAccounts(models.Model):
    userID = models.AutoField(primary_key=True)
    username = models.CharField(max_length=50)
    password = models.CharField(max_length=50)
    rating = models.IntegerField()
    movie = models.CharField(max_length=30)
    class Meta:
        db_table = 'userAccounts'  # Specify the desired table name in the database

    def __str__(self):
        return self.username

from django.db import models
from django.contrib.auth.models import User

class titleInfo(models.Model):
    titleID = models.AutoField(primary_key=True)
    title = models.CharField(max_length=100, null=True)
    genre = models.CharField(max_length=50, null=True)
    runtime = models.IntegerField(null=True)
    yearReleased = models.IntegerField(null=True)

    def __str__(self):
        return str(self.titleID)

    class Meta:
        db_table = 'titleInfo'


class Rating(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, default=None)
    title_info = models.ForeignKey(titleInfo, on_delete=models.CASCADE, default=None)
    rating = models.CharField(max_length=70)
    rated_date = models.DateTimeField(auto_now_add=True)

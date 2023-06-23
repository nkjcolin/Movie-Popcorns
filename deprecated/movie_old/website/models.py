from django.db import models

# Create your models here.

class TitleInfo(models.Model):
    title = models.CharField(max_length=100)
    genre = models.CharField(max_length=50)
    runtime = models.IntegerField(null=True)
    year_released = models.IntegerField(null=True)

    def __str__(self):
        return self.title

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

class castMap(models.Model):
    castID = models.IntegerField(primary_key=True)
    # Other cast-related fields here

    class Meta:
        db_table = 'castMap'


class titleCasts(models.Model):
    titleID = models.ForeignKey(titleInfo, on_delete=models.CASCADE)
    castID = models.ForeignKey(castMap, on_delete=models.CASCADE)
    # Other fields related to title-cast mapping

    class Meta:
        db_table = 'titleCasts'


class genreMap(models.Model):
    mappingID = models.AutoField(primary_key=True)
    genreID = models.IntegerField()
    titleID = models.IntegerField()

    genre = models.ForeignKey('titleGenres', on_delete=models.CASCADE)
    title = models.ForeignKey('titleInfo', on_delete=models.CASCADE)

    class Meta:
        db_table = 'genreMap'


class titleGenres(models.Model):
    genreID = models.AutoField(primary_key=True)
    genre = models.CharField(max_length=50)

    class Meta:
        db_table = 'titleGenres'
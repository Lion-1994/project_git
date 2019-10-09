import datetime

from django.db import models


class User(models.Model):
    '''用户模型'''
    SEX = (
        ('male', '男性'),
        ('female', '女性'),
        ('no', '无性'),
    )

    phonenum = models.CharField(max_length=20, unique=True, verbose_name='手机号')
    nickname = models.CharField(max_length=50, unique=True, verbose_name='昵称')
    sex = models.CharField(max_length=8, choices=SEX, verbose_name='性别')
    birth_year = models.IntegerField(default=2000, verbose_name='出生年')
    birth_month = models.IntegerField(default=1, verbose_name='出生月')
    birth_day = models.IntegerField(default=1, verbose_name='出生日')
    avatar = models.CharField(max_length=1024, verbose_name='个人形象的URL')
    location = models.CharField(max_length=16, verbose_name='常居地')

    @property
    def age(self):
        # 获取当前日期
        today = datetime.date.today()

        # 用出生年月日构造日期
        birth_date = datetime.date(self.birth_year, self.birth_month, self.birth_day)

        # 用当前日期 减去 出生日期 , 取日子 // 365
        return (today - birth_date).days // 365

    def to_dict(self):
        return {
            'phonenum': self.phonenum,
            'nickname': self.nickname,
            'sex': self.sex,
            'age': self.age,
            'avatar': self.avatar,
            'location': self.location,
        }

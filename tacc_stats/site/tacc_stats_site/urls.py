from django.conf.urls import patterns, include, url
import settings
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    #url(r'^$', 'tacc_stats_site.views.home', name='home'),
    url(r'^$', 'machine.views.dates', name='dates'),
    url(r'^media/(?P<path>.*)$', 'django.views.static.serve',
        {'document_root': settings.MEDIA_ROOT}),                       
    url(r'^comet/', include('comet.urls', namespace="comet"),name='comet'),
    url(r'^admin/', include(admin.site.urls)),
)

from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('download/', views.download_parquet, name='download_parquet'),
    path('refresh/', views.refresh_data, name='refresh_data'),
]
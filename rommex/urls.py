from django.contrib import admin
from django.urls import path, include
from django.contrib.auth import views as auth_views
from django.views.generic import RedirectView

urlpatterns = [
    path('admin/', admin.site.urls),
    # Rutas de login/logout para autenticación
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(next_page='login'), name='logout'),
    # Redirigir la raíz al login
    path('', RedirectView.as_view(pattern_name='login', permanent=False)),
    # Otras rutas protegidas
    path('', include('flights.urls')),
]
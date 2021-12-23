from django.urls import path
from . import views

urlpatterns = [
    path('v1.0/test-spec', views.TestSpec.as_view(), name='test_spec'),
    path('v1.0/spec-source', views.Spec_source.as_view(), name='spec_source'),
    path('v1.0/manual', views.Lot_aggregation_manual.as_view(), name='lot_aggregation_manual'),
    path('v1.0', views.Lot_Aggregation.as_view(), name='lot_aggregation'),
    # path('save-details/', views.save_details, name='save_details'),
    # path('save-details', views.save_details, name='save_details'),
    # path('save-details/<int:lot_id>', views.save_details, name='save_details'),
    # path('save-details/<int:lot_id>/', views.save_details, name='save_details'),
    # path('tutorials/', tutorial_list, name='tutorial_list'),
    # path('tutorials/<int:pk>', views.tutorial_detail, name='tutorial_detail'),
]

-- パーソナルトレーニング利用者の分析
SELECT 
    CASE 
        WHEN personal_training = 1 THEN 'あり'
        ELSE 'なし'
    END as personal_training_status,
    COUNT(*) as member_count,
    AVG(visit_per_week) as avg_weekly_visits,
    AVG(avg_time_in_gym) as avg_gym_time,
    COUNT(CASE WHEN uses_sauna = 1 THEN 1 END) as sauna_users
FROM gym_membership_python
GROUP BY personal_training
ORDER BY personal_training DESC;
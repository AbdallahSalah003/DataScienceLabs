# ─── Template for Task 1.1 ────────────────────────────────────────────────────
# Hint: Use INNER JOIN to get author name from the authors table
query1_1 = """
SELECT 
    b.title,
    a.name AS author_name,
    b.publication_year 
FROM books b INNER JOIN authors a ON b.author_id = a.author_id
WHERE b.genre = 'Fiction' AND b.publication_year > 1960
"""
df1_1 = pd.read_sql_query(query1_1, conn) 
df1_1.to_csv('task1_1.csv', index=False)

# ─── Template for Task 1.2 ────────────────────────────────────────────────────
# Hint: Use INNER JOIN with borrowings and GROUP BY member
query1_2 = """
SELECT 
    m.name AS member_name,
    m.email,
    COUNT(br.borrow_id) AS total_borrowings
FROM members m INNER JOIN borrowings br ON m.member_id = br.member_id
WHERE m.membership_type = 'student' 
GROUP BY m.member_id, m.name, m.email
ORDER BY total_borrowings DESC
"""
df1_2 = pd.read_sql_query(query1_2, conn)
df1_2.to_csv('task1_2.csv', index=False)

# ─── Template for Task 1.3 ────────────────────────────────────────────────────
# Hint: Join members with borrowings, GROUP BY membership_type, use HAVING
query1_3 = """
SELECT 
    m.membership_type,
    COUNT(DISTINCT m.member_id) AS total_members,
    SUM(br.fine_amount) AS total_fines,
    SUM(br.fine_amount) / COUNT(DISTINCT m.member_id) AS avg_fine_per_member
FROM members m INNER JOIN borrowings br ON m.member_id = br.member_id
GROUP BY m.membership_type
HAVING SUM(br.fine_amount) > 0
"""
df1_3 = pd.read_sql_query(query1_3, conn)
df1_3.to_csv('task1_3.csv', index=False)


# ─── Template for Task 2.1 ────────────────────────────────────────────────────
query2_1 = """
SELECT 
    b.title,
    a.name AS author_name,
    CASE
        WHEN COUNT(br.borrow_id) = 0 THEN 'Never Borrowed'
        ELSE COUNT(br.borrow_id)
    END AS times_borrowed,
    b.copies_available AS currently_available
FROM books b INNER JOIN authors a ON b.author_id = a.author_id LEFT JOIN borrowings br ON b.book_id = br.book_id
GROUP BY b.book_id, b.title, a.name, b.copies_available
ORDER BY COUNT(br.borrow_id) DESC
LIMIT 3
"""
df2_1 = pd.read_sql_query(query2_1, conn)
df2_1.to_csv('task2_1.csv', index=False)

# ─── Template for Task 2.2 ────────────────────────────────────────────────────
query2_2 = """
SELECT
    bk.title AS book_title,
    m.name AS borrower_name,
    br.borrow_date,
    br.due_date,
    CAST(JULIANDAY(CURRENT_DATE) - JULIANDAY(br.due_date) AS INTEGER) AS days_overdue,
    (JULIANDAY(CURRENT_DATE) - JULIANDAY(br.due_date)) * 2 AS estimated_fine
FROM borrowings br
INNER JOIN books bk ON br.book_id = bk.book_id
INNER JOIN members m ON br.member_id = m.member_id
WHERE br.return_date IS NULL
  AND br.due_date < CURRENT_DATE
ORDER BY days_overdue DESC
"""
df2_2 = pd.read_sql_query(query2_2, conn)
df2_2.to_csv('task2_2.csv', index=False)

# ─── Template for Task 3.1 ────────────────────────────────────────────────────
query3_1 = """
WITH member_borrowing_stats AS (
SELECT 
    m.member_id,
    m.name AS member_name,
    m.membership_type,
    COUNT(b.borrow_id) AS total_borrowings,
    COUNT(b.return_date) AS books_returned,
    SUM(CASE WHEN b.return_date IS NULL THEN 1 ELSE 0 END) AS books_still_borrowed,
    SUM(b.fine_amount) AS total_fines_paid
FROM members m
LEFT JOIN borrowings b ON m.member_id = b.member_id
GROUP BY m.member_id, m.name, m.membership_type
),
return_performance AS (
SELECT 
    member_id,
    SUM(CASE WHEN return_date IS NOT NULL AND return_date <= due_date THEN 1 ELSE 0 END) AS on_time_count
FROM borrowings
GROUP BY member_id
)
SELECT
    ms.member_name,
    ms.membership_type,
    ms.total_borrowings,
    ms.books_returned,
    ms.books_still_borrowed,
    IFNULL(ms.total_fines_paid, 0) AS total_fines_paid,
    ROUND(
        CASE 
            WHEN ms.books_returned = 0 THEN 0 
            ELSE (CAST(rp.on_time_count AS REAL) / ms.books_returned) * 100 
        END, 2
    ) AS on_time_return_rate,
    CASE 
        WHEN ms.total_borrowings = 0 THEN 'Inactive'
        WHEN ms.total_borrowings BETWEEN 1 AND 5 THEN 'Active'
        ELSE 'Very Active'
    END AS member_category
FROM member_borrowing_stats ms
LEFT JOIN return_performance rp ON ms.member_id = rp.member_id
ORDER BY ms.total_borrowings DESC;
"""
df3_1 = pd.read_sql_query(query3_1, conn)
df3_1.to_csv('task3_1.csv', index=False)
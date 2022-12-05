CREATE TYPE type_actions AS ENUM ('visit', 'click', 'scroll', 'move');
CREATE TYPE type_tag AS ENUM ('Sport', 'Politics', 'Business');

CREATE TABLE IF NOT EXISTS data_lk(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id_lk INT,
    fio VARCHAR(100) NOT NULL,
	dob DATE NOT NULL,
    doc DATE NOT NULL)
    

CREATE TABLE IF NOT EXISTS data_web(
	id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	user_id  INT,
	timestamp INT,
	actions type_actions NOT NULL,
    page_id INT,
    tag type_tag NOT NULL,
    sign BOOLEAN,
    CONSTRAINT user_id_fk
        FOREIGN KEY (user_id)
        REFERENCES data_lk(id)
        ON DELETE CASCADE)

INSERT INTO data_lk(
	id_lk,
	fio,
    dob,
    doc
)
VALUES
(101, 'Иванов Иван Иванович', '1990/7/5', '2016/8/1'),
(102, 'Александрова Александра Александровна', '1995/1/22', '2017/10/7'),
(103, 'Николаева Людмила Петрововна', '1991/5/2', '2017/1/17'),
(104, 'Петров Петр Петрович', '1998/8/13', '2013/5/27'),
(105, 'Сидоров Иван Петрович', '1994/3/10', '2018/11/9'),
(106, 'Степанов Степан Степаныч', '1989/6/11', '2020/9/19');

INSERT INTO data_web(
	user_id,
	timestamp,
    actions,
    page_id,
    tag,
    sign
)
VALUES
(1, 1667627126, 'visit', 101, 'Sport', False),
(1, 1667627286, 'scroll', 101, 'Sport', False),
(1, 1667627300, 'click', 101, 'Sport', False),
(1, 1667627505, 'visit', 102, 'Politics', False),
(1, 1667627565, 'click', 102, 'Politics', False),
(1, 1667627586, 'visit', 103, 'Sport', False),
(2, 1667728001, 'visit', 104, 'Politics', True),
(2, 1667728101, 'scroll', 104, 'Politics', True),
(2, 1667728151, 'click', 104, 'Politics', True),
(2, 1667728200, 'visit', 105, 'Business', True),
(2, 1667728226, 'click', 105, 'Business', True),
(2, 1667728317, 'visit', 106, 'Business', True),
(2, 1667728359, 'scroll', 106, 'Business', True),
(3, 1667828422, 'visit', 101, 'Sport', False),
(3, 1667828486, 'scroll', 101, 'Sport', False),
(4, 1667828505, 'visit', 106, 'Business', False),
(5, 1667828511, 'visit', 101, 'Sport', True),
(5, 1667828901, 'click', 101, 'Sport', True),
(5, 1667828926, 'visit', 102, 'Politics', True),
(5, 1667828976, 'click', 102, 'Politics', True),
(6, 1667728317, 'visit', 106, 'Business', True),
(6, 1667728359, 'scroll', 106, 'Business', True),
(6, 1667828422, 'visit', 101, 'Sport', False);
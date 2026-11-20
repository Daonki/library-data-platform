-- 적재 대상 DB(dw)와 계정 생성 (postgres 컨테이너 최초 기동 시 1회 실행)
CREATE USER dw WITH PASSWORD 'dw';
CREATE DATABASE dw OWNER dw;

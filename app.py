#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
TheMoviePredictor script
Author: Arnaud de Mouhy <arnaud@admds.net>
"""

import mysql.connector
import sys
import argparse
import csv
from pprint import pprint

def setSqlMode(cnx, cursor):
    query = """SET sql_mode ="" """
    cursor.execute(query)
    cnx.commit()

def connectToDatabase():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')

def disconnectDatabase(cnx):
    cnx.close()

def createCursor(cnx):
    return cnx.cursor(dictionary=True)

def closeCursor(cursor):    
    cursor.close()

def findQuery(table, id):
    return ("SELECT * FROM {} WHERE id = {}".format(table, id))

def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def insertPersonQuery(firstname, lastname):
    return (f"INSERT INTO people (firstname, lastname) VALUES ('{firstname}','{lastname}')")

def insertMovieQuery(title, original_title, synopsis, duration, origin_country, rating, production_budget, marketing_budget, release_date, is3d):
    return (f"INSERT INTO movies (title, original_title, synopsis, duration, origin_country, rating, production_budget, marketing_budget, release_date, is3d) VALUES ('{title}','{original_title}','{synopsis}','{duration}','{origin_country}','{rating}','{production_budget}','{marketing_budget}','{release_date}','{is3d}')")



def find(table, id):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = findQuery(table, id)
    cursor.execute(query)
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def findAll(table):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(findAllQuery(table))
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def printPerson(person):
    print("#{}: {} {}".format(person['id'], person['firstname'], person['lastname']))

def printMovie(movie):
    print("#{}: {} released on {}".format(movie['id'], movie['title'], movie['release_date']))

def insertPerson(firstname, lastname):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = insertPersonQuery(firstname, lastname)
    cursor.execute(query)
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)

def insertMovie(title, original_title, synopsis,
                duration, origin_country, rating,
                production_budget,
                marketing_budget, 
                release_date, is3d):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    setSqlMode(cnx, cursor)
    query = insertMovieQuery(title, original_title, synopsis, duration, origin_country, rating, production_budget, marketing_budget, release_date, is3d)
    cursor.execute(query)
    cnx.commit()
    closeCursor(cursor)
    disconnectDatabase(cnx)

parser = argparse.ArgumentParser(description='Process MoviePredictor data')

parser.add_argument('context', choices=['people', 'movies'], help='Le contexte dans lequel nous allons travailler')

action_subparser = parser.add_subparsers(title='action', dest='action')

list_parser = action_subparser.add_parser('list', help='Liste les entités du contexte')
list_parser.add_argument('--export' , help='Chemin du fichier exporté')

find_parser = action_subparser.add_parser('find', help='Trouve une entité selon un paramètre')
find_parser.add_argument('id' , help='Identifant à rechercher')

insert_parser = action_subparser.add_parser('insert', help='Ajoute une nouvelle entité à la table')
insert_parser.add_argument('--firstname', help='Prénom de la personne à ajouter')
insert_parser.add_argument('--lastname', help='Nom de la personne à ajouter')
insert_parser.add_argument('--title', help='Titre du film à ajouter')
insert_parser.add_argument('--original_title', help='Titre original du film à ajouter')
insert_parser.add_argument('--synopsis', help="Résumé du film à ajouter")
insert_parser.add_argument('--duration', help='Durée du film à ajouter')
insert_parser.add_argument('--origin_country',help="Pays d'origine du film à ajouter")
insert_parser.add_argument('--rating', help="Limite d'age du film à ajouter")
insert_parser.add_argument('--production_budget', help= "Budget de production du film à ajouter")
insert_parser.add_argument('--marketing_budget', help="Budget promo du film à ajouter")
insert_parser.add_argument('--release_date', help="Date de sortie en France du film à ajouter")
insert_parser.add_argument('--is3d', help="Vrai si le film à ajouter est disponible en 3D")

args = parser.parse_args()

if args.context == "people":
    if args.action == "list":
        people = findAll("people")
        if args.export:
            with open(args.export, 'w', encoding='utf-8', newline='\n') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
        else:
            for person in people:
                printPerson(person)
    if args.action == "find":
        peopleId = args.id
        people = find("people", peopleId)
        for person in people:
            printPerson(person)
    if args.action == 'insert':
        insertPerson(args.firstname, args.lastname)

if args.context == "movies":
    if args.action == "list":  
        movies = findAll("movies")
        for movie in movies:
            printMovie(movie)
    if args.action == "find":  
        movieId = args.id
        movies = find("movies", movieId)
        for movie in movies:
            printMovie(movie)
    if args.action == 'insert':
        insertMovie(args.title, args.original_title, args.synopsis, args.duration, args.origin_country, args.rating, args.production_budget, args.marketing_budget, args.release_date, args.is3d)
#!/usr/bin/env python3
# coding: utf-8
# File: complex_event.py
# Author: lhy<lhy_in_blcu@126.com,https://huangyong.github.io>
# Date: 18-9-29

import os
from collections import Counter
import re
from py2neo import Graph,Node,Relationship
from collections import Counter


class ComplexEvent:
    def __init__(self):
        self.g = Graph(
            host="127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            http_port=7474,  # neo4j 服务器监听的端口号
            user="lhy",  # 数据库user name，如果没有更改过，应该是neo4j
            password="lhy123")

    '''对标题进行噪声去除操作'''
    def modify_event(self, s):
        res1 = re.compile('\[[^\[]*\]')
        # 移除《》标签
        res2 = re.compile('《[^《]*》')
        # 移除()标签
        res3 = re.compile(('\([^\(]*\)'))
        s = s.replace('：', ':').replace('丨', ':').replace('【', '[').replace('】', ']').replace('（', '(').replace('）',
                                                                                                                ')').replace(
            '\n', '')
        # 根据三条正则表达式，移除相应标签
        s = res1.sub('', s)
        s = res2.sub('', s)
        s = res3.sub('', s)
        ss = s.split(':')
        if len(ss) == 1:
            return s
        else:
            return ''.join(ss[1:])

    '''收集单一事件'''
    def collect_event(self):
        #只有###在重视环保、生态和移民问题的基础上，水电###才###能真正成为“清洁”能源
        f = open('event_seq_clean.txt', 'w+')
        event_list = []
        count = 0
        for line in open('event_seq.txt'):
            count += 1
            if count % 10000 == 0:
                print(count)
            line = line.strip()
            if not line:
                continue
            es = line.split('###')
            pre_wd = es[0]
            pre_part = [i for i in es[1].replace('，',',').split(',') if i]
            post_wd = es[2]
            post_part = [i for i in es[3].replace('，',',').split(',') if i]
            if len(pre_part) == 1 and len(post_part) == 1:
                pre_part = self.modify_event(pre_part[0])
                post_part = self.modify_event(post_part[0])
                if not pre_part or not post_part:
                    continue
                event = [pre_wd, pre_part, post_wd, post_part]
                event_list.append('###'.join(event))
        event_dict = Counter(event_list).most_common()
        for i in event_dict:
            f.write(i[0] + '\t' + str(i[1]) + '\n')
        f.close()

    '''获取代表事件'''
    def collect_type_dict(self):
        f = open('event_condition_typical.txt', 'w+')
        for line in open('event_condition_clean.txt'):
            line = line.strip()
            if not line:
                continue
            tmp = line.split('\t')[0]
            es = tmp.split('###')
            pre_wd = es[0]
            pre_part = es[1]
            post_wd = es[2]
            post_part = es[3]
            if 1< len(pre_part) < 9 and 1<len(post_part) < 10:
                f.write(line + '\n')
        f.close()

    '''插入neo4j数据库'''
    def collect_data_condition(self):
        nodes =[]
        edges = []
        index = 0
        for line in open('event_condition_typical.txt',):
            index += 1
            line = line.strip().split('\t')
            if not line:
                continue
            count = int(line[1])
            if count < 2:
                continue
            es = line[0].split('###')
            pre_wd = es[0]
            pre_part = es[1]
            post_wd = es[2]
            post_part = es[3]
            nodes.append(pre_part)
            nodes.append(post_part)
            label = '-'.join([pre_wd, post_wd])
            edges.append([pre_part, label, post_part])
        nodes_dict = Counter(nodes).most_common()
        return nodes_dict, edges

    '''插入neo4j数据库'''
    def collect_data_but(self):
        nodes =[]
        edges = []
        index = 0
        for line in open('event_but_typical.txt',):
            index += 1
            line = line.strip().split('\t')
            if not line:
                continue
            count = int(line[1])
            if count < 2:
                continue
            es = line[0].split('###')
            pre_wd = es[0]
            pre_part = es[1]
            post_wd = es[2]
            post_part = es[3]
            nodes.append(pre_part)
            nodes.append(post_part)
            label = '-'.join([pre_wd, post_wd])
            edges.append([pre_part, label, post_part])
        nodes_dict = Counter(nodes).most_common()
        return nodes_dict, edges

    '''插入节点'''
    def create_but_nodes(self):
        i = 0
        nodes_dict, edges = self.collect_data_but()
        all = len(nodes_dict)
        for node in nodes_dict:
            node_name = node[0]
            node_count = node[1]
            try:
                node = Node("Event", name=node_name, count=node_count)
                self.g.create(node)
            except Exception as e:
                print(e)
            i += 1
            print(i,'/',all)

    '''因果事件关系边'''
    def create_but_edge(self):
        nodes_dict, edges = self.collect_data_but()
        for rel in edges:
            p = rel[0]
            q = rel[2]
            label = rel[1]
            if p == q:
                continue
            query = "match(p:Event),(q:Event) where p.name='%s'and q.name='%s' create (p)-[rel:inverese_with{label:'%s'}]->(q)" % (
            p, q, label)
            print(query)
            try:
                self.g.run(query)
            except Exception as e:
                print(e)
        return

    '''插入节点'''
    def create_condition_nodes(self):
        i = 0
        nodes_dict, edges = self.collect_data_condition()
        all = len(nodes_dict)
        for node in nodes_dict:
            node_name = node[0]
            node_count = node[1]
            try:
                check_exist = "match (p:Event) where p.name='%s' return p" % (node_name)
                status = self.g.run(check_exist).data()
                if not status:
                    node = Node("Event", name=node_name, count=node_count)
                    self.g.create(node)
            except Exception as e:
                print(e)
            i += 1
            print(i, '/', all)


    '''因果事件关系边'''
    def create_condition_edge(self):
        nodes_dict, edges = self.collect_data_condition()
        for rel in edges:
            p = rel[0]
            q = rel[2]
            label = rel[1]
            if p == q:
                continue
            query = "match(p:Event),(q:Event) where p.name='%s'and q.name='%s' create (p)-[rel:if_so{label:'%s'}]->(q)" % (
                p, q, label)
            print(query)
            try:
                self.g.run(query)
            except Exception as e:
                print(e)
        return


handler = ComplexEvent()
#创建相反节点
handler.create_but_nodes()
#创建条件节点
handler.create_condition_nodes()
#创建条件边
handler.create_condition_edge()
#创建相反边
handler.create_but_edge()

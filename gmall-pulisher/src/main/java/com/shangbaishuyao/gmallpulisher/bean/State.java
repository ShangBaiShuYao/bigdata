package com.shangbaishuyao.gmallpulisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class State {
    String title;
    List<Option> options;
}
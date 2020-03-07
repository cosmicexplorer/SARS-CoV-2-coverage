jar_library(
  name='stanford-ner',
  jars=[
    jar(org='edu.stanford', name='nlp', rev='???',
        url='file:///Users/dmcclanahan/projects/active/sars-cov-2-coverage/stanford-ner-2018-10-16/stanford-ner-3.9.2.jar'),
  ],
)

jvm_binary(
  name='ner-demo',
  sources=['*.java'],
  dependencies=[
    ':stanford-ner',
  ],
  main='NERDemo',
  extra_jvm_options=['-Xmx4g'],
)
